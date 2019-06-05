/*
 * Copyright 2018 The CovenantSQL Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// Package logictestparser implements sqllogictest parser,
// 	for more see: https://sqlite.org/sqllogictest/doc/trunk/about.wiki
package main

import (
	"database/sql"
	"fmt"
	"github.com/CovenantSQL/CovenantSQL/client"
	"github.com/CovenantSQL/CovenantSQL/utils/log"
	"io/ioutil"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
)

// RecordType is Statement or Query
type RecordType int

// MatchList is list of map[groupName]RegexMatchedString
type MatchList []map[string]string

const (
	// Statement is an SQL command that is to be evaluated but from which
	// 	we do not expect to get results (other than success or failure).
	// 	A statement might be a CREATE TABLE or an INSERT or an UPDATE or a DROP INDEX.
	Statement RecordType = iota
	// Query is an SQL command from which we expect to receive results.
	// 	The result set might be empty.
	Query
	// Halt can be inserted after a query that is giving an anomalous result,
	// 	causing the database to be left in the state where it gives the unexpected answer.
	// 	After sqllogictest exist, manually debugging can then proceed.
	Halt
)

// QueryRecord is query record:
// 	query <type-string> <sort-mode> <label>
//
// 	The <type-string> argument to the query statement is a short string that specifies the number
// 	of result columns and the expected datatype of each result column. There is one character in
// 	the <type-string> for each result column. The characters codes are "T" for a text result,
// 	"I" for an integer result, and "R" for a floating-point result.
//
// 	The <sort-mode> argument is optional. If included, it must be one of "nosort", "rowsort", or
// 	"valuesort". The default is "nosort". In nosort mode, the results appear in exactly the order
// 	in which they were received from the database engine. The nosort mode should only be used on
// 	queries that have an ORDER BY clause or which only have a single row of result, since otherwise
// 	the order of results is undefined and might vary from one database engine to another. The
// 	"rowsort" mode gathers all output from the database engine then sorts it by rows on the
// 	client side. Sort comparisons use strcmp() on the rendered ASCII text representation of the
// 	values. Hence, "9" sorts after "10", not before. The "valuesort" mode works like rowsort
// 	except that it does not honor row groupings. Each individual result value is sorted on its
// 	own.
//
// 	The <label> argument is also optional. If included, sqllogictest stores a hash of the results
// 	of this query under the given label. If the label is reused, then sqllogictest verifies that
// 	the results are the same. This can be used to verify that two or more queries in the same
// 	test script that are logically equivalent always generate the same output.
type QueryRecord struct {
	TypeString string
	SortMode   string
	Label      string
	SQL        string
	Result     string
}

// StatementRecord begins with one of the following two lines:
// 	statement ok
//	statement error
type StatementRecord struct {
	OK  bool
	SQL string
}

// Record is StatementRecord or QueryRecord
type Record struct {
	Type      RecordType
	Query     *QueryRecord
	Statement *StatementRecord
	SkipIf    string // eg. SkipIf sqlite # empty RHS
	OnlyIf    string // eg. onlyif sqlite # empty RHS
}

// SQLLogicTestSuite is SQL logic test records list
type SQLLogicTestSuite struct {
	ml      MatchList
	Records []*Record
}

// Parse parses the test file and returns a MatchList
func (slt *SQLLogicTestSuite) Parse(file string) (err error) {
	c, err := ioutil.ReadFile(file)
	if err != nil {
		return
	}
	raw := strings.Replace(string(c), "\r\n", "\n", -1)
	pattern := regexp.MustCompile(
		`(?ms)(?:(?:(?P<if>skipif|onlyif)\s+(?P<condition>.+?)\s*(?:#.*?){0,1}\n){0,1})` +
			`(?:(?P<halt>halt)|(?:(?P<type>statement|query)\s+(?P<rconf>.*?)\n(?P<record>.*?)(?:----(?P<result>.*?))?)\n\n)`)
	matches := pattern.FindAllStringSubmatch(raw, -1)
	if len(matches) > 0 {
		ml := make(MatchList, 0, len(matches))
		records := make([]*Record, 0, len(ml))

		for _, v := range matches {
			m := make(map[string]string)
			for i, g := range pattern.SubexpNames() {
				if len(g) > 0 {
					m[g] = v[i]
				}
			}

			r := new(Record)
			switch m["if"] {
			case "skipif":
				r.SkipIf = m["condition"]
			case "onlyif":
				r.OnlyIf = m["condition"]
			}
			if len(m["halt"]) != 0 {
				// Halt is just to abort
				r.Type = Halt
			} else if m["type"] == "statement" {
				// statement can be:
				// 	statement ok
				//	statement error
				r.Type = Statement
				r.Statement = new(StatementRecord)
				switch m["rconf"] {
				case "ok":
					r.Statement.OK = true
				case "error":
					r.Statement.OK = false
				}
				r.Statement.SQL = m["record"]
			} else if m["type"] == "query" {
				// query <type-string> <sort-mode> <label>
				r.Type = Query
				r.Query = new(QueryRecord)
				r.Query.SQL = m["record"]
				r.Query.Result = m["result"]
				rconf := strings.Split(m["rconf"], " ")
				for i, v := range rconf {
					switch i {
					case 0:
						r.Query.TypeString = v
					case 1:
						r.Query.SortMode = v
					case 2:
						r.Query.Label = v
					}
				}
			}
			records = append(records, r)

			ml = append(ml, m)
		}

		slt.ml = ml
		slt.Records = records
	}

	return
}

func Path(baseDir string) (files []string) {
	FJ := filepath.Join
	files = make([]string, 0)
	// select
	for i := 1; i < 6; i++ {
		files = append(files, FJ(baseDir, fmt.Sprintf("select%d.test", i)))
	}

	// evidence
	evidence := FJ(baseDir, "evidence")
	files = append(files, fmt.Sprintf("%s/in1.test", evidence))
	files = append(files, fmt.Sprintf("%s/in2.test", evidence))
	files = append(files, fmt.Sprintf("%s/slt_lang_aggfunc.test", evidence))
	files = append(files, fmt.Sprintf("%s/slt_lang_createtrigger.test", evidence))
	files = append(files, fmt.Sprintf("%s/slt_lang_createview.test", evidence))
	files = append(files, fmt.Sprintf("%s/slt_lang_dropindex.test", evidence))
	files = append(files, fmt.Sprintf("%s/slt_lang_droptable.test", evidence))
	files = append(files, fmt.Sprintf("%s/slt_lang_droptrigger.test", evidence))
	files = append(files, fmt.Sprintf("%s/slt_lang_dropview.test", evidence))
	files = append(files, fmt.Sprintf("%s/slt_lang_reindex.test", evidence))
	files = append(files, fmt.Sprintf("%s/slt_lang_replace.test", evidence))
	files = append(files, fmt.Sprintf("%s/slt_lang_update.test", evidence))

	// index
	index := FJ(baseDir, "index")

	// index/between ----------------------------------------------------------------------
	between := fmt.Sprintf("%s/between", index)
	// index/between/1
	files = append(files, fmt.Sprintf("%s/1/slt_good_0.test", between))
	// index/between/10
	between10 := fmt.Sprintf("%s/10", between)
	for i := 0; i < 6; i++ {
		files = append(files, fmt.Sprintf("%s/slt_good_%d.test", between10, i))
	}
	// index/between/100
	between100 := fmt.Sprintf("%s/100", between)
	for i := 0; i < 5; i++ {
		files = append(files, fmt.Sprintf("%s/slt_good_%d.test", between100, i))
	}
	// index/between/1000
	files = append(files, fmt.Sprintf("%s/1000/slt_good_0.test", between))

	// index/commute ----------------------------------------------------------------------
	commute := fmt.Sprintf("%s/commute", index)
	// index/commute/10
	commute10 := fmt.Sprintf("%s/10", commute)
	for i := 0; i < 35; i++ {
		files = append(files, fmt.Sprintf("%s/slt_good_%d.test", commute10, i))
	}
	// index/commute/100
	commute100 := fmt.Sprintf("%s/100", commute)
	for i := 0; i < 13; i++ {
		files = append(files, fmt.Sprintf("%s/slt_good_%d.test", commute100, i))
	}
	// index/commute/1000
	commute1000 := fmt.Sprintf("%s/1000", commute)
	for i := 0; i < 4; i++ {
		files = append(files, fmt.Sprintf("%s/slt_good_%d.test", commute1000, i))
	}

	// index/delete ----------------------------------------------------------------------
	indexDelete := fmt.Sprintf("%s/delete", index)
	// index/delete/1
	files = append(files, fmt.Sprintf("%s/1/slt_good_0.test", indexDelete))
	// index/delete/10
	delete10 := fmt.Sprintf("%s/10", indexDelete)
	for i := 0; i < 6; i++ {
		files = append(files, fmt.Sprintf("%s/slt_good_%d.test", delete10, i))
	}
	// index/delete/100
	delete100 := fmt.Sprintf("%s/100", indexDelete)
	for i := 0; i < 4; i++ {
		files = append(files, fmt.Sprintf("%s/slt_good_%d.test", delete100, i))
	}
	// index/delete/1000
	delete1000 := fmt.Sprintf("%s/1000", indexDelete)
	for i := 0; i < 2; i++ {
		files = append(files, fmt.Sprintf("%s/slt_good_%d.test", delete1000, i))
	}
	// index/delete/10000
	files = append(files, fmt.Sprintf("%s/10000/slt_good_0.test", indexDelete))

	// index/in ----------------------------------------------------------------------
	in := fmt.Sprintf("%s/in", index)
	// index/in/10
	in10 := fmt.Sprintf("%s/10", in)
	for i := 0; i < 6; i++ {
		files = append(files, fmt.Sprintf("%s/slt_good_%d.test", in10, i))
	}
	// index/in/100
	in100 := fmt.Sprintf("%s/100", in)
	for i := 0; i < 5; i++ {
		files = append(files, fmt.Sprintf("%s/slt_good_%d.test", in100, i))
	}
	// index/in/1000
	in1000 := fmt.Sprintf("%s/1000", in)
	for i := 0; i < 2; i++ {
		files = append(files, fmt.Sprintf("%s/slt_good_%d.test", in1000, i))
	}

	// index/orderby ----------------------------------------------------------------------
	orderby := fmt.Sprintf("%s/orderby", index)
	// index/orderby/10
	orderby10 := fmt.Sprintf("%s/10", orderby)
	for i := 0; i < 26; i++ {
		files = append(files, fmt.Sprintf("%s/slt_good_%d.test", orderby10, i))
	}
	// index/orderby/100
	orderby100 := fmt.Sprintf("%s/100", orderby)
	for i := 0; i < 4; i++ {
		files = append(files, fmt.Sprintf("%s/slt_good_%d.test", orderby100, i))
	}
	// index/orderby/1000
	files = append(files, fmt.Sprintf("%s/1000/slt_good_0.test", orderby))

	// index/orderby_nosort ----------------------------------------------------------------------
	orderbyNosort := fmt.Sprintf("%s/orderby_nosort", index)
	// index/orderby_nosort/10
	orderbyNosort10 := fmt.Sprintf("%s/10", orderbyNosort)
	for i := 0; i < 40; i++ {
		files = append(files, fmt.Sprintf("%s/slt_good_%d.test", orderbyNosort10, i))
	}
	// index/orderby_nosort/100
	orderbyNosort100 := fmt.Sprintf("%s/100", orderbyNosort)
	for i := 0; i < 7; i++ {
		files = append(files, fmt.Sprintf("%s/slt_good_%d.test", orderbyNosort100, i))
	}
	// index/orderby_nosort/1000
	orderbyNosort1000 := fmt.Sprintf("%s/1000", orderbyNosort)
	for i := 0; i < 2; i++ {
		files = append(files, fmt.Sprintf("%s/slt_good_%d.test", orderbyNosort1000, i))
	}

	// index/random ----------------------------------------------------------------------
	indexRandom := fmt.Sprintf("%s/random", index)
	// index/random/10
	random10 := fmt.Sprintf("%s/10", indexRandom)
	for i := 0; i < 15; i++ {
		files = append(files, fmt.Sprintf("%s/slt_good_%d.test", random10, i))
	}
	// index/random/100
	random100 := fmt.Sprintf("%s/100", indexRandom)
	for i := 0; i < 2; i++ {
		files = append(files, fmt.Sprintf("%s/slt_good_%d.test", random100, i))
	}
	// index/random/1000
	random1000 := fmt.Sprintf("%s/1000", indexRandom)
	for i := 0; i < 9; i++ {
		files = append(files, fmt.Sprintf("%s/slt_good_%d.test", random1000, i))
	}

	// index/view ----------------------------------------------------------------------
	view := fmt.Sprintf("%s/view", index)
	// index/view/10
	view10 := fmt.Sprintf("%s/10", view)
	for i := 0; i < 8; i++ {
		files = append(files, fmt.Sprintf("%s/slt_good_%d.test", view10, i))
	}
	// index/view/100
	view100 := fmt.Sprintf("%s/100", view)
	for i := 0; i < 6; i++ {
		files = append(files, fmt.Sprintf("%s/slt_good_%d.test", view100, i))
	}
	// index/view/1000
	files = append(files, fmt.Sprintf("%s/1000/slt_good_0.test", view))
	// index/view/10000
	files = append(files, fmt.Sprintf("%s/10000/slt_good_0.test", view))

	// random
	random := FJ(baseDir, "random")

	// random/aggregates ----------------------------------------------------------------------
	for i := 0; i < 130; i++ {
		files = append(files, fmt.Sprintf("%s/aggregates/slt_good_%d.test", random, i))
	}

	// random/expr ----------------------------------------------------------------------
	for i := 0; i < 120; i++ {
		files = append(files, fmt.Sprintf("%s/expr/slt_good_%d.test", random, i))
	}

	// random/groupby ----------------------------------------------------------------------
	for i := 0; i < 14; i++ {
		files = append(files, fmt.Sprintf("%s/groupby/slt_good_%d.test", random, i))
	}

	// random/select ----------------------------------------------------------------------
	for i := 0; i < 127; i++ {
		files = append(files, fmt.Sprintf("%s/select/slt_good_%d.test", random, i))
	}

	return files
}

func Check(file string, records []*Record) {
	for i, v := range records {
		//fmt.Println("type: ", v.Type)
		//fmt.Println("OnlyIf: ", v.OnlyIf)
		//fmt.Println("SkipIf: ", v.SkipIf)
		if v.OnlyIf == "" || v.OnlyIf == "sqlite" || v.SkipIf != "sqlite" {
			switch v.Type {
			case Halt:

			case Statement:
				statement := v.Statement
				//fmt.Println(statement.OK, "--", statement.SQL)
				_, err := db.Exec(statement.SQL)
				if (err != nil && statement.OK) || (err == nil && !statement.OK) {
					log.Errorf("check sql fail, file: %s, sql: %s, err: %v", file, statement.SQL, err)
				}
			case Query:
				query := v.Query
				//fmt.Println("TypeString: ", query.TypeString)
				//fmt.Println("SortMode: ", query.SortMode)
				//fmt.Println("Label: ", query.Label)
				//fmt.Println("SQL: ", query.SQL)
				//fmt.Println("Result: ", query.Result)
				_, err := db.Query(query.SQL)
				// TODO check query result
				if err != nil {
					log.Errorf("check sql fail, file: %s, sql: %s, err: %v", file, query.SQL, err)
				}
			}
		}
		fmt.Println("-------------------------", i, file)
	}
}

// TODO every Check should use a new db.
var db *sql.DB

func init() {
	config := "/Users/xueyuwei/.cql/config.yaml"
	password := ""
	dsn := "covenantsql://8b7e13b14865fa7b7d39cbdb284ef8d8832e97ed8c54e25a27db628a4e551f3e"
	err := client.Init(config, []byte(password))
	if err != nil {
		log.Fatal(err)
	}

	// Connect to database instance
	db, err = sql.Open("covenantsql", dsn)
	if err != nil {
		log.Fatal(err)
	}
}

func main() {
	baseDir := "/Users/xueyuwei/tmp/sqllogictest/test"
	files := Path(baseDir)

	length := len(files)
	count := 8
	num := length/count + 1
	index := 0
	wg := sync.WaitGroup{}
	for i := 0; i < num; i++ {
		wg.Add(count)
		for j := 0; j < count; j++ {
			index++
			go func(i int) {
				defer wg.Done()
				if i < length {
					fmt.Println(files[i])
					slt := &SQLLogicTestSuite{}
					err := slt.Parse(files[i])
					if err != nil {
						log.Errorf("read test file %s failed: %v", files[i], err)
					}
					// check sql
					Check(files[i], slt.Records)
				}
			}(index)
		}
		wg.Wait()
	}
}
