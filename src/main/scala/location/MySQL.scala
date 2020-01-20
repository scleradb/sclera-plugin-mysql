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

package com.scleradb.plugin.dbms.rdbms.mysql.location

import com.scleradb.exec.Schema

import com.scleradb.sql.mapper.SqlMapper

import com.scleradb.dbms.location.{LocationId, LocationPermit}
import com.scleradb.dbms.rdbms.driver.SqlDriver
import com.scleradb.dbms.rdbms.location.RdbmsLocation

import com.scleradb.plugin.dbms.rdbms.mysql.mapper.MySQLMapper
import com.scleradb.plugin.dbms.rdbms.mysql.driver.MySQLDriver

class MySQL(
    override val schema: Schema,
    override val id: LocationId,
    override val dbName: String,
    baseConfig: List[(String, String)],
    override val permit: LocationPermit
) extends RdbmsLocation {
    private val jdbcDriverClass: Class[com.mysql.jdbc.Driver] =
        classOf[com.mysql.jdbc.Driver]

    override val isTemporary: Boolean = false
    override val dbms: String = MySQL.id
    override val param: String =
        if( dbName contains "/" ) dbName else "localhost/" + dbName
    override val dbSchemaOpt: Option[String] = None

    override val sqlMapper: SqlMapper = new MySQLMapper(this)
    override val url: String = "jdbc:mysql://" + param
    override val config: List[(String, String)] =
        (("useOldAliasMetadataBehavior", "true")::baseConfig).distinct

    override def driver: SqlDriver =
        new MySQLDriver(this, sqlMapper, url, config)
}

object MySQL {
    val id: String = "MYSQL"
}
