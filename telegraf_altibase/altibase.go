package altibase

import (
	"database/sql"
	"fmt"
	_ "github.com/alexbrainman/odbc"
	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/plugins/inputs"
	"github.com/influxdata/toml"
	"io/ioutil"
	"os"
	"sync"
)

type MonQuery struct {
	SeriesName string   `toml:"series_name"`
	Sql        string   `toml:"sql"`
	Tags       []string `toml:"tags"`
	Fields     []string `toml:"fields"`
	Pivot      bool     `toml:"pivot"`
	PivotKey   string   `toml:"pivot_key"`
	Enable     bool     `toml:"enable"`
}

type MonitorElements struct {
	Title    string     `toml:"title"`
	MonQuery []MonQuery `toml:"monitor_query"`
}

type Altibase struct {
	Dsn          string `toml:"altibase_dsn"`
	Server       string `toml:"altibase_server"`
	Port         int    `toml:"altibase_port"`
	User         string `toml:"altibase_user"`
	Password     string `toml:"altibase_password"`
	QueryVersion string `toml:"altibase_queryversion"`
	QueryFile    string `toml:"altibase_queryfile"`
}

var monquery MonitorElements
var isInitialized = false

func initQueries(m *Altibase) {

	f, err := os.Open(m.QueryFile)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	buf, err := ioutil.ReadAll(f)
	if err != nil {
		panic(err)
	}

	if err := toml.Unmarshal(buf, &monquery); err != nil {
		panic(err)
	}

	isInitialized = true
}

func init() {
	inputs.Add("altibase", func() telegraf.Input {
		return &Altibase{}
	})
}

var sampleConfig = `

## specify connection string
altibase_dsn    = "Altiodbc" 
altibase_server = "127.0.0.1" 
altibase_port   = 20300
altibase_user   = "sys"
altibase_password = "manager"
altibase_queryversion="V6"
altibase_queryfile="query.toml"
`

func (m *Altibase) BuildConnectionString() string {

	sConnectionString := fmt.Sprintf("DSN=%s;SERVER=%s;PORT=%d;USER=%s;PASSWORD=%s", m.Dsn, m.Server, m.Port, m.User, m.Password)
	return sConnectionString
}

func (m *Altibase) SampleConfig() string {
	return sampleConfig
}

func (m *Altibase) Description() string {
	return "Read metrics from one altibase server ( per instance ) "
}

func (m *Altibase) GatherServer(acc telegraf.Accumulator) error {
	return nil
}

func (m *Altibase) Gather(acc telegraf.Accumulator) error {

	var wg sync.WaitGroup
	connectionString := m.BuildConnectionString()

	if m.Dsn == "" {
		return nil
	}

	if !isInitialized {
		initQueries(m)
	}

	// Loop through each server and collect metrics
	wg.Add(1)
	go func(s string) {
		defer wg.Done()
		acc.AddError(m.gatherServer(s, acc))
	}(connectionString)

	wg.Wait()

	return nil
}

func (m *Altibase) getCommonTags(db *sql.DB) map[string]string {

	v := make(map[string]string)

	return v
}

func (m *Altibase) runSQL(acc telegraf.Accumulator, db *sql.DB) error {

	for _, element := range monquery.MonQuery {
		if !element.Enable {
			continue
		}
		tags := m.getCommonTags(db)
		fields := make(map[string]interface{})

		r, err := m.getSQLResult(db, element.Sql)
		if err != nil {
			return err
		}

		if element.Pivot {

			for _, v := range r {
				for _, v2 := range element.Tags {
					tags[v2] = v[v2].(string)
				}

				key := v[element.PivotKey].(string)
				data := v[element.Fields[1]]
				fields[key] = data
			}
			acc.AddFields(element.SeriesName, fields, tags)

		} else {

			for _, v := range r {
				for _, v2 := range element.Tags {
					tags[v2] = v[v2].(string)
				}

				for _, v2 := range element.Fields {
					fields[v2] = v[v2]
				}
				acc.AddFields(element.SeriesName, fields, tags)

			}
		}
	}

	return nil
}

func (m *Altibase) getSQLResult(db *sql.DB, sqlText string) ([]map[string]interface{}, error) {
	rows, err := db.Query(sqlText)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	columns, err := rows.Columns()
	column_count := len(columns)

	result_data := make([]map[string]interface{}, 0)
	value_data := make([]interface{}, column_count)
	value_ptrs := make([]interface{}, column_count)

	for rows.Next() {

		for i := 0; i < column_count; i++ {
			value_ptrs[i] = &value_data[i]
		}

		rows.Scan(value_ptrs...)
		entry := make(map[string]interface{})

		for i, col := range columns {
			var v interface{}
			val := value_data[i]

			b, ok := val.([]byte)

			if ok {
				v = string(b)
			} else {
				v = val
			}
			entry[col] = v
		}
		result_data = append(result_data, entry)
	}
	return result_data, nil

}

func (m *Altibase) gatherServer(serv string, acc telegraf.Accumulator) error {

	db, err := sql.Open("odbc", serv)
	if err != nil {
		return err
	}

	err = m.runSQL(acc, db)
	if err != nil {
		return err
	}

	defer db.Close()

	return nil
}
