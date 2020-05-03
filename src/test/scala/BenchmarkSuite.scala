/**
* Sclera - MySQL Connector
* Copyright 2012 - 2020 Sclera, Inc.
* 
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
* 
*     http://www.apache.org/licenses/LICENSE-2.0
* 
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

package com.scleradb.test

import org.scalatest.CancelAfterFailure
import org.scalatest.funspec.AnyFunSpec

import java.util.Properties

import java.sql.{Connection, Statement, ResultSet, ResultSetMetaData, Types}
import java.sql.{DriverManager, SQLException}

import com.scleradb.interfaces.jdbc.ScleraJdbcDriver
import com.scleradb.sqltests.runner.SqlTestRunner

class BenchmarkSuite
extends AnyFunSpec with CancelAfterFailure with SqlTestRunner {
    val jdbcUrl: String = "jdbc:scleradb"

    var conn: Connection = null
    var stmt: Statement = null

    describe("JDBC driver") {
        it("should setup") {
            val props: Properties = new Properties()
            props.setProperty("schemaDbms", "H2MEM")
            props.setProperty("schemaDb", "scleraschema")

            conn = DriverManager.getConnection(jdbcUrl, props)
            stmt = conn.createStatement()

            if( conn.getWarnings() != null ) stmt.executeUpdate("create schema")
        }

        it("should add mysql location") {
            stmt.executeUpdate(
                "add location scleratest as mysqltest('scleratest')"
            )
        }

        it("should make the location default") {
            stmt.executeUpdate("set default location = scleratest")
        }

        it("should create a test table") {
            stmt.executeUpdate("create table T(col varchar) as values ('bar')")
        }

        it("should execute a test query") {
            val rs: ResultSet = stmt.executeQuery("select col as foo from t")
            val metaData: ResultSetMetaData = rs.getMetaData()

            assert(metaData.getColumnCount() === 1)
            assert(metaData.getColumnName(1) === "FOO")
            assert(
                (metaData.getColumnType(1) == Types.VARCHAR) ||
                (metaData.getColumnType(1) == Types.CHAR)
            )

            assert(rs.next() === true)
            assert(rs.getString(1) === "bar")
            assert(rs.getString("foo") === "bar")
            assert(rs.getString(rs.findColumn("foo")) === "bar")
            assert(rs.next() === false)

            rs.close()
        }

        it("should drop the test table") {
            stmt.executeUpdate("drop table t")
        }

        it("should execute CREATE script") {
            runScript(stmt, "/scripts/create.out")
        }

        it("should execute INT4 script") {
            runScript(stmt, "/scripts/int4.out")
        }

        it("should execute INT8 script") {
            runScript(stmt, "/scripts/int8.out")
        }

        it("should execute FLOAT8 script") {
            runScript(stmt, "/scripts/float8.out")
        }

        it("should execute JOIN script") {
            runScript(stmt, "/scripts/join.out")
        }

        it("should execute AGGREGATES script") {
            runScript(stmt, "/scripts/aggregates.out")
        }

        it("should execute HAVING script on table") {
            runScript(stmt, "/scripts/select_having.out")
        }

        it("should execute MISC script") {
            runScript(stmt, "/scripts/misc.out")
        }

        it("should execute DROP script") {
            runScript(stmt, "/scripts/drop.out")
        }

        it("should reset default") {
            stmt.executeUpdate("set default location = tempdb")
        }

        it("should remove mysql location") {
            stmt.executeUpdate("remove location scleratest")
        }

        it("should close the statement") {
            stmt.close()

            val e: Throwable = intercept[SQLException] {
                stmt.executeQuery("select 1::int as foo")
            }

            assert(e.getMessage() === "Statement closed")
        }

        it("should close the connection") {
            conn.close()

            val e: Throwable = intercept[SQLException] {
                conn.createStatement(
                    ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY
                )
            }

            assert(e.getMessage() === "Connection closed")
        }
    }
}
