package immutable

import immutable.encoders.{Dict, Dense}

import scala.collection.mutable.MutableList
import scala.io.Source
import jawn._
import jawn.ast._

/**
  * Created by marcin on 2/17/16.
  */
case class Table(name: String, columns: Column*) extends Serializable {
    private val _columns: Map[String, Column] = columns.map(x => (x.name, x)).toMap
    private var _size = refreshSize

    def size: Int = _size

    def refreshSize: Int = {
        val sizeFile = Source.fromFile(s"${Config.home}/$name/_size")
        _size = sizeFile.getLines.next.toInt
        sizeFile.close()
        _size
    }

    def column[A](name: String) = {
        _columns.get(name) match {
            case Some(x) => x.asInstanceOf[A]
            case _ => throw new Exception(s"Column of name ${name} does not exists in this table.")
        }
    }
}

object Table {
    def loadTable(tblName: String): Table = {
        val parsed = jawn.ast.JParser.parseFromPath(s"${Config.home}/$tblName/_table.json").get
        val parsedCols = parsed.get("columns")

        var columns = MutableList[Column]()
        var continueWhile = true
        var counter = 0

        while (continueWhile) {
            if (parsedCols.get(counter).isInstanceOf[JObject]) {
                val col = parsedCols.get(counter)
                val name: String = col.get("name").asString
                val size: Int = col.get("size").getBigInt match {
                    case Some(x) => x.toInt
                    case None => 0
                }
                val encoder: Symbol = col.get("encoder").getString match {
                    case Some(x) => Symbol(x)
                    case None => 'Dense
                }

                val newColumn = col.get("type").asString match {
                    case "TinyInt" => TinyIntColumn(name, tblName, Dense)
                    case "ShortInt" => ShortIntColumn(name, tblName, Dense)
                    case "Int" => IntColumn(name, tblName, Dense)
                    case "FixedChar" => encoder match {
                        case 'Dense => FixedCharColumn(name, tblName, size, Dense)
                        case 'Dict => FixedCharColumn(name, tblName, size, Dict)
                        case _ => throw new Exception("Encoder not compatible with this data type.")
                    }
                    case "VarChar" => VarCharColumn(name, tblName, size, Dict)
                    case _ => throw new Exception(s"Column definition not recognized: ${col.get("type").asString}")
                }

                columns += newColumn
                counter += 1
            } else {
                continueWhile = false
            }
        }

        Table(tblName, columns: _*)
    }
}
