package Impl

import Objects.UserAccountService.SafeUserInfo
import APIs.UserAuthService.VerifyTokenValidityMessage
import APIs.UserAccountService.QuerySafeUserInfoByTokenMessage
import APIs.UserAccountService.QuerySafeUserInfoByUserIDListMessage
import Objects.SystemLogService.SystemLogEntry
import Objects.UserAccountService.UserRole
import Common.API.{PlanContext, Planner}
import Common.DBAPI._
import Common.Object.SqlParameter
import Common.ServiceUtils.schemaName
import org.slf4j.LoggerFactory
import org.joda.time.DateTime
import io.circe._
import io.circe.syntax._
import io.circe.generic.auto._
import cats.effect.IO
import cats.implicits._

case class QuerySystemLogsMessagePlanner(
                                          adminToken: String,
                                          fromTimestamp: Option[DateTime],
                                          toTimestamp: Option[DateTime],
                                          userIDs: List[Int],
                                          override val planContext: PlanContext
                                        ) extends Planner[List[SystemLogEntry]] {
  val logger = LoggerFactory.getLogger(this.getClass.getSimpleName + "_" + planContext.traceID.id)

  override def plan(using PlanContext): IO[List[SystemLogEntry]] = {
    for {
      // Step 1.1: Verify adminToken validity
      _ <- IO(logger.info("开始验证管理员权限"))
      isAdmin <- VerifyTokenValidityMessage(adminToken).send
      _ <- if (!isAdmin) 
             IO.raiseError(new Exception("管理员权限验证失败")) 
           else
             IO(logger.info("管理员权限验证成功"))

      // Step 1.2: Verify user role is admin
      _ <- IO(logger.info("开始验证管理员角色权限"))
      safeUserInfoOpt <- QuerySafeUserInfoByTokenMessage(adminToken).send
      _ <- safeUserInfoOpt match {
        case None => IO.raiseError(new Exception("管理员角色验证失败"))
        case Some(safeUserInfo) =>
          if (safeUserInfo.role != UserRole.SuperAdmin) 
            IO.raiseError(new Exception("管理员角色验证失败")) 
          else 
            IO(logger.info("管理员角色验证成功"))
      }

      // Step 2.1: Validate userIDs if provided
      _ <- if (userIDs.nonEmpty) 
             validateUserIDs(userIDs) 
           else 
             IO(logger.info("未提供需要验证的userIDs，跳过userIDs验证"))

      // Step 2.2: Query system logs
      _ <- IO(logger.info("开始查询日志记录"))
      logEntries <- querySystemLogEntries()

      // Step 3: Sort results by timestamp and return
      sortedLogEntries <- IO { logEntries.sortBy(_.timestamp.getMillis) }
      _ <- IO(logger.info(s"查询完成，返回的日志数量：${sortedLogEntries.length}"))
    } yield sortedLogEntries
  }

  // 验证userIDs是否有效
  private def validateUserIDs(userIDs: List[Int])(using PlanContext): IO[Unit] = {
    for {
      _ <- IO(logger.info("开始验证userIDs的有效性"))
      safeUserInfos <- QuerySafeUserInfoByUserIDListMessage(userIDs).send
      validUserIDs = safeUserInfos.map(_.userID)
      invalidUserIDs = userIDs.diff(validUserIDs)
      _ <- if (invalidUserIDs.nonEmpty) 
             IO.raiseError(new Exception(s"无效的userIDs: ${invalidUserIDs.mkString(", ")}")) 
           else 
             IO(logger.info("所有userIDs均有效"))
    } yield ()
  }

  // 查询系统日志
  private def querySystemLogEntries()(using PlanContext): IO[List[SystemLogEntry]] = {
    val baseSQL = s"SELECT * FROM ${schemaName}.system_log_table"
    val conditions = buildConditions()
    val sqlQuery = if (conditions.isEmpty) 
                     baseSQL 
                   else 
                     s"$baseSQL WHERE ${conditions.mkString(" AND ")}"

    for {
      _ <- IO(logger.info(s"构建的SQL查询：$sqlQuery"))
      parameters = buildSqlParameters()
      _ <- IO(logger.info(s"构建的SQL参数：${parameters.map(p => s"(${p.dataType}, ${p.value})").mkString(", ")}"))

      rows <- readDBRows(sqlQuery, parameters)
      logEntries <- IO(rows.map(decodeType[SystemLogEntry]))
    } yield logEntries
  }

  // 构建SQL过滤条件
  private def buildConditions()(using PlanContext): List[String] = {
    val conditions = scala.collection.mutable.ListBuffer.empty[String]

    if (fromTimestamp.isDefined && toTimestamp.isDefined) {
      conditions += "(timestamp >= ? AND timestamp <= ?)"
    } else if (fromTimestamp.isDefined) {
      conditions += "timestamp >= ?"
    } else if (toTimestamp.isDefined) {
      conditions += "timestamp <= ?"
    }

    if (userIDs.nonEmpty) {
      conditions += "user_id = ANY(?)"
    }

    conditions.toList
  }

  // 构建SQL查询参数
  private def buildSqlParameters()(using PlanContext): List[SqlParameter] = {
    val parameters = scala.collection.mutable.ListBuffer.empty[SqlParameter]

    fromTimestamp.foreach { ft => 
      parameters += SqlParameter("DateTime", ft.getMillis.toString)
    }
    toTimestamp.foreach { tt => 
      parameters += SqlParameter("DateTime", tt.getMillis.toString)
    }
    if (userIDs.nonEmpty) {
      parameters += SqlParameter("Array[Int]", userIDs.asJson.noSpaces)
    }

    parameters.toList
  }
}
// 模型无法修复编译错误的原因: QuerySafeUserInfoByTokenMessage 未提供具体的定义或导入路径