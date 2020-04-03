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

package com.scleradb.plugin.dbms.rdbms.mysql.mapper

import com.scleradb.util.tools.Counter

import com.scleradb.objects._

import com.scleradb.sql.objects._
import com.scleradb.sql.datatypes._
import com.scleradb.sql.types._
import com.scleradb.sql.expr._
import com.scleradb.sql.statements._
import com.scleradb.sql.mapper._
import com.scleradb.sql.mapper.target._

import com.scleradb.dbms.rdbms.location.RdbmsLocation

class MySQLMapper(loc: RdbmsLocation) extends SqlMapper {

    override def queryString(
        query: SqlRelQueryStatement
    ): String = targetQueryString(SqlTranslator.translateQuery(query))

    private def targetQueryString(
        targetQuery: TargetSqlQuery
    ): String = targetQuery match {
        case TargetSqlSelect(distinct, select, from, where,
                             group, having, order,
                             limit, offset, isAggregate) =>
            val distinctClause: Option[String] = distinct.map {
                case Nil => "DISTINCT"
                case _ =>
                    throw new IllegalArgumentException(
                        "DISTINCT ON is not supported in MySQL"
                    )
            }

            val selectClause: Option[String] =
                Some(select.map(expr => targetExprString(expr)).mkString(", "))

            val fromClause: Option[String] = Some("FROM " + fromString(from))

            val whereClause: Option[String] =
                where.map { predExpr => "WHERE " + exprString(predExpr) }

            val groupClause: Option[String] = group match {
                case Nil =>
                    if( isAggregate ) Some("GROUP BY NULL") else None
                case exprs =>
                    Some("GROUP BY " +
                         exprs.map(expr => exprString(expr)).mkString(", "))
            }

            val havingClause: Option[String] =
                having.map { predExpr => "HAVING " + exprString(predExpr) }

            val orderClause: Option[String] = order match {
                case Nil => None
                case exprs =>
                    Some("ORDER BY " +
                         exprs.map(expr => sortExprString(expr)).mkString(", "))
            }

            val limitClause: Option[String] = limit.map { n => "LIMIT " + n }

            val offsetClause: Option[String] = offset.map { n => "OFFSET " + n }

            val clauses: List[Option[String]] =
                List(Some("SELECT"), distinctClause, selectClause,
                     fromClause, whereClause,
                     groupClause, havingClause,
                     orderClause, limitClause, offsetClause)

            clauses.flatten.mkString(" ")

        case TargetSqlCompound(compoundType, lhs, rhs) =>
            val compoundTypeStr: String = compoundType match {
                case Union => "UNION"
                case Intersect => "INTERSECT"
                case Except => "EXCEPT"
            }

            "(" + targetQueryString(lhs) + ") " + compoundTypeStr +
            " ALL (" + targetQueryString(rhs) + ")"
    }

    private def fromString(from: TargetSqlFrom): String = from match {
        case TargetSqlTableRef(name, Nil) => name

        case TargetSqlTableRef(name, cols) =>
            name + "(" + cols.map(col => exprString(col)).mkString(", ") + ")"

        case TargetSqlValues(name, cols, rows) if( !rows.isEmpty ) =>
            // rewrite needed as MySQl does not allow VALUES in the FROM clause
            val extCols: List[ColRef] =
                cols:::rows.head.scalars.drop(cols.size).map { _ =>
                    ColRef(Counter.nextSymbol("V"))
                }

            val rowQueriesStrs: List[String] = rows.map { row => 
                val targetStrs: List[String] = row.scalars.zip(extCols).map {
                    case (scal, col) => targetExprString(AliasedExpr(scal, col))
                }

                "SELECT " + targetStrs.mkString(", ")
            }

            val valuesQueryString: String =
                rowQueriesStrs.tail.foldLeft (rowQueriesStrs.head) {
                    case (prev, qs) => "(" + prev + ") UNION ALL (" + qs + ")"
                }

            "(" + valuesQueryString + ") AS " + name

        case TargetSqlJoin(joinType, joinPred, lhsInput, rhsInput) =>
            val joinTypeStr: String = joinType match {
                case Inner => "INNER JOIN"
                case FullOuter => "FULL OUTER JOIN"
                case LeftOuter => "LEFT OUTER JOIN"
                case RightOuter => "RIGHT OUTER JOIN"
            }

            val lhsStr: String = fromString(lhsInput)
            val rhsStr: String = fromString(rhsInput)
            joinPred match {
                case JoinOn(predExpr) =>
                    lhsStr + " " + joinTypeStr + " " + rhsStr + " ON (" +
                        exprString(predExpr) +
                    ")"
                case JoinUsing(cols) =>
                    lhsStr + " " + joinTypeStr + " " + rhsStr + " USING (" +
                        cols.map(col => exprString(col)).mkString(", ") +
                    ")"
                case JoinNatural =>
                    lhsStr + " NATURAL " + joinTypeStr + " " + rhsStr
            }

        case TargetSqlNested(name, query) =>
            "(" + targetQueryString(query) + ") AS " + name

        case _ =>
            throw new RuntimeException("Cannot map (MySQL): " + from)
    }

    override def updateString(
        stmt: SqlUpdateStatement
    ): List[String] = stmt match {
        case SqlCreateDbObject(obj, Persistent) =>
            List("CREATE " + objectString(obj))

        case SqlCreateDbObject(obj, Temporary) =>
            List("CREATE TEMPORARY " + objectString(obj))

        case SqlDropExplicit(st: SchemaTable, _) =>
            val tableTypeStr: String = st.obj.baseType match {
                case Table.BaseTable => "TABLE"
                case Table.BaseView => "VIEW"
            }

            List("DROP " + tableTypeStr + " IF EXISTS " + st.obj.name)

        case SqlCreateIndex(indexName, relationId, indexColRefs, pred) =>
            if( pred == BoolConst(true) )
                List(
                    "CREATE INDEX " + indexName +
                    " ON " + relationId.name + "(" +
                        indexColRefs.map(c => exprString(c)).mkString(", ") +
                    ")"
                )
            else throw new IllegalArgumentException(
                "Partial index not supported in MySQL"
            )

        case SqlDropIndex(indexName) =>
            List("DROP INDEX " + indexName)

        case SqlInsertValueRows(TableId(_, name), targetCols, rows) =>
            List(
                "INSERT INTO " +
                fromString(
                    TargetSqlTableRef(name, targetCols)
                ) + " VALUES " +
                rows.map(row => "(" + exprString(row) + ")").mkString(", ")
            )

        case SqlInsertQueryResult(tableId, targetCols, Values(_, rows)) =>
            updateString(SqlInsertValueRows(tableId, targetCols, rows))

        case SqlInsertQueryResult(TableId(_, name), targetCols, relExpr) =>
            List(
                "INSERT INTO " +
                fromString(
                    TargetSqlTableRef(name, targetCols)
                ) + " " + queryString(SqlRelQueryStatement(relExpr))
            )
        case SqlUpdate(tableId, cvs, pred) =>
            val setExprs: List[ScalExpr] =
                cvs.map { case (c, v) => ScalOpExpr(Equals, List(c, v)) }
            List(
                "UPDATE " + tableId.name +
                " SET " + setExprs.map(e => exprString(e)).mkString(", ") +
                " WHERE " + exprString(pred)
            )

        case SqlDelete(tableId, pred) =>
            List("DELETE FROM " + tableId.name + " WHERE " + exprString(pred))

        case SqlUpdateBatch(stmts) =>
            stmts.flatMap { stmt => updateString(stmt) }

        case _ =>
            throw new RuntimeException("Cannot map (MySQL): " + stmt)
    }

    private def objectString(obj: SqlDbObject): String = obj match {
        case SqlObjectAsExpr(name, logExpr, DbMaterialized(_)) =>
            "TABLE " + name + " AS " +
            queryString(SqlRelQueryStatement(logExpr))

        case SqlObjectAsExpr(name, logExpr, DbVirtual) =>
            "VIEW " + name + " AS " + queryString(SqlRelQueryStatement(logExpr))

        case SqlTable(table, _, None) =>
            val pkCols: List[String] = table.keyOpt match {
                case Some(PrimaryKey(cols)) => cols.map { col => col.name }
                case None => Nil
            }

            val fkCols: List[String] = table.foreignKeys.flatMap {
                case ForeignKey(cols, _, _, _) => cols.map { col => col.name }
            }

            val indexCols: List[String] = pkCols:::fkCols

            val colStrs: List[String] = table.columns.map {
                case Column(colName, colType, None) =>
                    val colTypeStr: String =
                        if( indexCols contains colName ) {
                            // the type needs to be index-friendly
                            colType match {
                                case SqlOption(t) => sqlTypeIndexString(t)
                                case t => sqlTypeIndexString(t) + " NOT NULL"
                            }
                        } else {
                            colType match {
                                case SqlOption(t) => sqlTypeString(t)
                                case t => sqlTypeString(t) + " NOT NULL"
                            }
                        }

                    colName + " " + colTypeStr
                case _ =>
                    throw new RuntimeException("Column families not handled")
            }

            val keyStrs: List[String] = table.keyOpt.toList.map {
                case PrimaryKey(cols) =>
                    "PRIMARY KEY (" +
                        cols.map(col => exprString(col)).mkString(", ") +
                    ")"
            }

            val refStrs: List[String] =
                table.foreignKeys.filter { fk =>
                    fk.refTableId(loc.schema).locationId == loc.id
                } map { case ForeignKey(cols, _, refTableName, refCols) =>
                    "FOREIGN KEY (" +
                        cols.map(col => exprString(col)).mkString(", ") +
                    ") REFERENCES " +
                    fromString(TargetSqlTableRef(refTableName, refCols))
                }

            "TABLE " + table.name +
                "(" + (colStrs:::keyStrs:::refStrs).mkString(", ") + ")"

        case _ =>
            throw new RuntimeException("Cannot map (MySQL): " + obj)
    }

    private def exprString(scalExpr: ScalExpr): String = scalExpr match {
        case ColRef(name) => name

        case AnnotColRef(Some(tName), cName) => tName + "." + cName

        case AnnotColRef(None, cName) => cName

        case IntConst(value) => value.toString

        case ShortConst(value) => value.toString

        case LongConst(value) => value.toString

        case FloatConst(value) => value.toString

        case DoubleConst(value) => value.toString

        case BoolConst(value) => value.toString

        case CharConst(value) =>
            "'" + value.replaceAll("'", "''") + "'"

        case DateConst(value) => "'" + value.toString + "'"

        case TimestampConst(value) => "'" + value.toString + "'"

        case TimeConst(value) => "'" + value.toString + "'"

        case SqlTypedNull(t) => "CAST(NULL AS " + sqlTypeCastString(t) + ")"

        case Pattern(pat, esc) => pat + " ESCAPE " + esc

        case Row(vs) => vs.map(s => exprString(s)).mkString(", ")

        case ScalSubQuery(relExpr) => queryString(SqlRelQueryStatement(relExpr))

        case Exists(relExpr) =>
            "EXISTS (" + queryString(SqlRelQueryStatement(relExpr)) + ")"

        case ScalOpExpr(cmpOp: ScalRelCmpOp,
                        List(lhs, ScalCmpRelExpr(qual, subQueryOrList))) =>
            val cmpQualStr: String = qual match {
                case CmpAll => "ALL"
                case CmpAny => "ANY"
            }

            "(" + exprString(lhs) + ") " +
            cmpOpString(cmpOp) + " " + cmpQualStr +
            " (" + relSubQueryString(subQueryOrList) + ")"

        case ScalOpExpr(TypeCast(t), List(input)) =>
            "CAST(" + exprString(input) + " AS " + sqlTypeCastString(t) + ")"

        case ScalOpExpr(UnaryPlus, List(input)) =>
            "+(" + exprString(input) + ")"

        case ScalOpExpr(UnaryMinus, List(input)) =>
            "-(" + exprString(input) + ")"

        case ScalOpExpr(Plus, List(lhs, rhs)) =>
            "(" + exprString(lhs) + ") + (" + exprString(rhs) + ")"

        case ScalOpExpr(Minus, List(lhs, rhs)) =>
            "(" + exprString(lhs) + ") - (" + exprString(rhs) + ")"

        case ScalOpExpr(Mult, List(lhs, rhs)) =>
            "(" + exprString(lhs) + ") * (" + exprString(rhs) + ")"

        case ScalOpExpr(Div, List(lhs, rhs)) =>
            "(" + exprString(lhs) + ") / (" + exprString(rhs) + ")"

        case ScalOpExpr(Modulo, List(lhs, rhs)) =>
            "(" + exprString(lhs) + ") % (" + exprString(rhs) + ")"

        case ScalOpExpr(Exp, List(lhs, rhs)) =>
            "(" + exprString(lhs) + ") ^ (" + exprString(rhs) + ")"

        case ScalOpExpr(Not, List(input)) =>
            "NOT (" + exprString(input) + ")"

        case ScalOpExpr(Or, List(lhs, rhs)) =>
            "(" + exprString(lhs) + ") OR (" + exprString(rhs) + ")"

        case ScalOpExpr(And, List(lhs, rhs)) =>
            "(" + exprString(lhs) + ") AND (" + exprString(rhs) + ")"

        case ScalOpExpr(IsNull, List(input)) =>
            "(" + exprString(input) + ") IS NULL"

        case ScalOpExpr(IsLike(pat), List(lhs)) =>
            "(" + exprString(lhs) + ") LIKE " + exprString(pat)

        case ScalOpExpr(IsILike(pat), List(lhs)) =>
            "(" + exprString(lhs) + ") ILIKE " + exprString(pat)

        case ScalOpExpr(IsSimilarTo(pat), List(lhs)) =>
            "(" + exprString(lhs) + ") SIMILAR TO " + exprString(pat)

        case ScalOpExpr(IsDistinctFrom, List(lhs, rhs)) =>
            "(" + exprString(lhs) +
            ") IS DISTINCT FROM (" + exprString(rhs) + ")"

        case ScalOpExpr(IsBetween(qual), List(expr, lhs, rhs)) =>
            val rangeQualStr: String = qual match {
                case Symmetric => "SYMMETRIC"
                case Asymmetric => ""
            }

            "(" + exprString(expr) + ") IS BETWEEN " + rangeQualStr +
            " (" + exprString(lhs) + ") AND (" + exprString(rhs) + ")"

        case ScalOpExpr(cmpOp: ScalRelCmpOp, List(lhs, rhs)) =>
            "(" + exprString(lhs) + ") " + cmpOpString(cmpOp) +
            " (" + exprString(rhs) + ")"

        case ScalOpExpr(AggregateFunction(name, qual), inputs) =>
            val funcQualStr: String = qual match {
                case FuncAll => ""
                case FuncDistinct => "DISTINCT "
            }

            functionAlias(name) + "(" +
                funcQualStr + inputs.map(e => exprString(e)).mkString(", ") +
            ")"

        case ScalOpExpr(ScalarFunction("DATE_PART"), List(spec, d)) =>
            spec match {
                case CharConst(evalSpec)
                if evalSpec.toUpperCase == "DAY_OF_WEEK" =>
                    "DAYOFWEEK(" + exprString(d) + ")"
                case CharConst(evalSpec)
                if evalSpec.toUpperCase == "DAY_OF_YEAR" =>
                    "DAYOFYEAR(" + exprString(d) + ")"
                case CharConst(evalSpec) =>
                    "EXTRACT(" + evalSpec + " FROM " + exprString(d) + ")"
                case _ =>
                    throw new IllegalArgumentException(
                        "Cannot compute DATE_PART on " +
                        "unknown specification \"" + spec.repr + "\""
                    )
            }

        case ScalOpExpr(ScalarFunction(name), inputs) =>
            functionAlias(name) +
            "(" + inputs.map(e => exprString(e)).mkString(", ") + ")"

        case CaseExpr(argExpr, whenThen, defaultExpr) =>
            List(
                List("CASE", exprString(argExpr)),
                whenThen.flatMap {
                    case (w, t) =>
                        List("WHEN", exprString(w), "THEN", exprString(t))
                },
                List("ELSE", exprString(defaultExpr), "END")
            ).flatten.mkString(" ")

        case _ => throw new RuntimeException("Cannot map (MySQL): " + scalExpr)
    }

    private def cmpOpString(cmpOp: ScalRelCmpOp): String = cmpOp match {
        case Equals => "="
        case NotEquals => "<>"
        case LessThan => "<"
        case LessThanEq => "<="
        case GreaterThan => ">"
        case GreaterThanEq => ">="
    }

    private def targetExprString(
        targetExpr: TargetExpr
    ): String = targetExpr match {
        case StarTargetExpr(Some((tname, None)), Nil) => tname + ".*"
        case StarTargetExpr(None, Nil) => "*"
        case (t: ScalarTarget) =>
            "(" + exprString(t.expr) + ") AS " + exprString(t.alias)
        case _ =>
            throw new RuntimeException("Cannot map (MySQL): " + targetExpr)
    }

    private def sortExprString(sortExpr: SortExpr): String = sortExpr match {
        case SortExpr(expr, sortDir, nullsOrder) =>
            val sortDirStr: String = sortDir match {
                case SortAsc => "ASC"
                case SortDesc => "DESC"
            }

            val exprStr: String = exprString(expr)

            nullsOrder match {
                case NullsFirst =>
                    "(" + exprStr + ") " + sortDirStr
                case NullsLast =>
                    "ISNULL(" + exprStr + "), (" + exprStr + ") " + sortDirStr
            }
    }

    private def relSubQueryString(
        subQueryOrList: RelSubQueryBase
    ): String = subQueryOrList match {
        case ScalarList(exprs) =>
            val rows: List[Row] = exprs.map {
                case (v: ScalValue) => Row(List(v))
                case _ =>
                    throw new IllegalArgumentException(
                        "Only scalar values expected in the list"
                    )
            }

            targetQueryString(
                TargetSqlSelect(
                    from = TargetSqlValues(Counter.nextSymbol("Q"), Nil, rows)
                )
            )

        case RelSubQuery(relExpr) => queryString(SqlRelQueryStatement(relExpr))
    }

    private def sqlTypeString(sqlType: SqlType): String = sqlType match {
        case SqlInteger => "INTEGER"
        case SqlSmallInt => "SMALLINT"
        case SqlBigInt => "BIGINT"
        case SqlDecimal(None, None) => "DECIMAL(65, 30)"
        case SqlDecimal(Some(p), None) => "DECIMAL(" + p + ")"
        case SqlDecimal(Some(p), Some(s)) => "DECIMAL(" + p + ", " + s + ")"
        case SqlFloat(None) => "FLOAT"
        case SqlFloat(Some(p)) => "FLOAT(" + p + ")"
        case SqlReal => "REAL"
        case SqlBool => "BOOLEAN"
        case SqlCharFixed(None) => "CHAR BINARY"
        case SqlCharFixed(Some(l)) => "CHAR(" + l + ") BINARY"
        case SqlCharVarying(None) =>
            "VARCHAR(" + SqlType.maxVarCharLen + ") BINARY"
        case SqlCharVarying(Some(l)) => "VARCHAR(" + l + ") BINARY"
        case SqlText => "TEXT"
        case SqlTimestamp => "TIMESTAMP"
        case SqlTime => "TIME"
        case SqlDate => "DATE"
        case SqlOption(baseType) => sqlTypeString(baseType)
        case _ => throw new RuntimeException("Cannot map (MySQL): " + sqlType)
    }

    // MySQl places restrictions on the types in CAST(...)
    private def sqlTypeCastString(
        sqlType: SqlType
    ): String = sqlType match {
        case SqlInteger => "SIGNED"
        case SqlSmallInt => "SIGNED"
        case SqlBigInt => "SIGNED"
        case SqlDecimal(None, None) => "DECIMAL(65, 30)"
        case SqlDecimal(Some(p), None) => "DECIMAL(" + p + ")"
        case SqlDecimal(Some(p), Some(s)) => "DECIMAL(" + p + ", " + s + ")"
        case SqlFloat(_) => "DECIMAL(65, 30)"
        case SqlReal => "DECIMAL(65, 30)"
        case SqlBool => "CHAR(1)"
        case SqlCharFixed(None) => "CHAR BINARY"
        case SqlCharFixed(Some(l)) => "CHAR(" + l + ") BINARY"
        case SqlCharVarying(None) => "CHAR BINARY"
        case SqlCharVarying(Some(l)) => "CHAR(" + l + ") BINARY"
        case SqlText => "CHAR BINARY"
        case SqlTimestamp => "DATETIME"
        case SqlTime => "TIME"
        case SqlDate => "DATE"
        case SqlOption(baseType) => sqlTypeCastString(baseType)
        case _ => throw new RuntimeException("Cannot map (MySQL): " + sqlType)
    }

    private def sqlTypeIndexString(
        sqlType: SqlType
    ): String = sqlType match {
        case SqlInteger => "INTEGER"
        case SqlSmallInt => "SMALLINT"
        case SqlBigInt => "BIGINT"
        case SqlDecimal(None, None) => "DECIMAL(65, 30)"
        case SqlDecimal(Some(p), None) => "DECIMAL(" + p + ")"
        case SqlDecimal(Some(p), Some(s)) => "DECIMAL(" + p + ", " + s + ")"
        case SqlFloat(None) => "FLOAT"
        case SqlFloat(Some(p)) => "FLOAT(" + p + ")"
        case SqlReal => "REAL"
        case SqlBool => "BOOLEAN"
        case SqlCharFixed(None) => "CHAR BINARY"
        case SqlCharFixed(Some(l)) => "CHAR(" + l + ") BINARY"
        case (SqlCharVarying(None) | SqlText) =>
            "VARCHAR(" + SqlType.maxVarCharLen + ") BINARY"
        case SqlCharVarying(Some(l)) => "VARCHAR(" + l + ") BINARY"
        case SqlTimestamp => "TIMESTAMP"
        case SqlTime => "TIME"
        case SqlDate => "DATE"
        case SqlOption(baseType) => sqlTypeIndexString(baseType)
        case _ => throw new RuntimeException("Cannot map (MySQL): " + sqlType)
    }

    override val functionMapOpt: Option[Map[String, String]] = Some(
        Map(
            "ABS" -> "ABS",
            "AVG" -> "AVG",
            "CEIL" -> "CEILING",
            "CEILING" -> "CEILING",
            "CHARACTER_LENGTH" -> "CHARACTER_LENGTH",
            "CHAR_LENGTH" -> "CHARACTER_LENGTH",
            "COALESCE" -> "IFNULL",
            "CONCAT" -> "CONCAT",
            "COUNT" -> "COUNT",
            "DATE_PART" -> "DATE_PART",
            "EXP" -> "EXP",
            "FLOOR" -> "FLOOR",
            "LENGTH" -> "LENGTH",
            "LOG" -> "LOG",
            "LOG10" -> "LOG10",
            "LOWER" -> "LOWER",
            "MAX" -> "MAX",
            "MIN" -> "MIN",
            "MOD" -> "MOD",
            "NULLIF" -> "NULLIF",
            "POWER" -> "POWER",
            "RANDOM" -> "RAND",
            "REPLACE" -> "REPLACE",
            "ROUND" -> "ROUND",
            "SIGN" -> "SIGN",
            "SQRT" -> "SQRT",
            "STDDEV" -> "STDDEV_SAMP",
            "STDDEV_POP" -> "STDDEV_POP",
            "STDDEV_SAMP" -> "STDDEV_SAMP",
            "SUBSTRING" -> "SUBSTRING",
            "SUM" -> "SUM",
            "TRIM" -> "TRIM",
            "TRUNC" -> "TRUNC",
            "UPPER" -> "UPPER",
            "VARIANCE" -> "VAR_SAMP",
            "VAR_POP" -> "VAR_POP",
            "VAR_SAMP" -> "VAR_SAMP",
            "CURRENT_DATE" -> "CURRENT_DATE",
            "CURRENT_TIME" -> "CURRENT_TIME",
            "CURRENT_TIMESTAMP" -> "CURRENT_TIMESTAMP",
            "ASIN" -> "ASIN",
            "ACOS" -> "ACOS",
            "ATAN" -> "ATAN",
            "ATAN2" -> "ATAN2",
            "SIN" -> "SIN",
            "COS" -> "COS",
            "TAN" -> "TAN"
        )
    )
}
