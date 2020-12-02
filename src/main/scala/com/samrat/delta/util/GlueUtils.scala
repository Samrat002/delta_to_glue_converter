//package com.samrat.delta.util
//
//import software.amazon.awssdk.regions.Region
//import software.amazon.awssdk.services.glue.GlueClient
//import software.amazon.awssdk.services.glue.model.{CreateTableRequest, DeleteTableRequest, EntityNotFoundException, GetTableRequest, SerDeInfo, StorageDescriptor, TableInput, UpdateTableRequest}
//import java.net.URI
//import java.util.concurrent.TimeUnit._
//
//import com.samrat.delta.util.{GlueUtils, GlueUtilsTypes, ZkUtils}
//import org.apache.hadoop.conf.Configuration
//import org.apache.hadoop.fs.{FileSystem, Path}
//import org.apache.hadoop.hive.metastore.api.{Partition, SerDeInfo, StorageDescriptor}
//import org.apache.spark.sql.{DataFrame, SparkSession}
//import org.apache.spark.sql.catalyst.TableIdentifier
//import org.apache.spark.sql.catalyst.catalog._
//import org.apache.spark.sql.delta.DeltaLog
//import org.apache.spark.sql.delta.DeltaTableUtils._
//import org.apache.spark.sql.delta.storage.LogStore
//import org.apache.spark.sql.functions._
//import org.apache.spark.sql.hive.HiveUtils
//import org.apache.spark.sql.types._
//import org.json4s._
//import org.json4s.jackson.Serialization
//import org.json4s.jackson.Serialization.write
//import org.slf4j.{Logger, LoggerFactory}
//import software.amazon.awssdk.services.glue.GlueClient
//
//import scala.collection.JavaConverters._
//import scala.collection.mutable
//import scala.util.control.NonFatal
//import scala.util.{Failure, Success, Try}
//import scala.util.Try
//
//object GlueUtils {
//  import GlueUtilsTypes._
//  import GlueUtilsPrivate._
//
//  def createPartition(table:TableDetails, partitions:Fields, location:String)
//                     (implicit glueClient:GlueClient):Try[Unit] = {
//    Try{
//      //add partition here
//    }
//  }
//
//  def dropPartition() (implicit glueClient:GlueClient):Try[Unit] = {
//    Try{
//      //Implementation here
//    }
//  }
//
//  def checkIfPartitionExists() (implicit glueClient:GlueClient):Try[Unit] = {
//    Try{
//      //Implementation here
//    }
//  }
//
//  //TODO: Implement this
//  //  def getPartition(FieldInfos:Fields):(S3FilesList, PartitionDetails) = {
//
//  //    val req = GetTableRequest.builder().databaseName(tableDetails.db).name(tableDetails.name).build()
//  //    Try { glueClient.getTable(req) }.map(_.table().name() == tableDetails.name).recover{
//  //      case _:EntityNotFoundException => false
//  //    }
//  //  }
//
//  def createTable(
//                   tableDetails:TableDetails, partitionKeys: Fields,
//                   fields:Fields, location:String, fileFormat: GlueFileFormat
//                 )
//                 (implicit glueClient:GlueClient):Try[Unit] = Try{
//
//    val tableStorage = createStorageDescriptor(location, fields, fileFormat)
//    val tableInput:TableInput = TableInput.builder()
//      .name(tableDetails.name)
//      .tableType(TABLE_TYPE_EXTERNAL)
//      .partitionKeys( fieldInfoToGlueType(partitionKeys):_* )
//      .storageDescriptor(tableStorage)
//      .build()
//
//    val createTableRequest = CreateTableRequest.builder()
//      .databaseName(tableDetails.db)
//      .tableInput(tableInput)
//      .build()
//
//    glueClient.createTable(createTableRequest)
//
//  }
//
//  def alterTableLocation(tableDetails:TableDetails, location:String,
//                         partitions: Option[Seq[String]] = None)
//                        (implicit glueClient:GlueClient):Try[Unit] = Try {
//    val req = GetTableRequest.builder()
//      .databaseName(tableDetails.db)
//      .name(tableDetails.name)
//      .build()
//    val getTableResp = glueClient.getTable(req)
//    val table = getTableResp.table()
//
//    val partitionKs: java.util.List[GlueColumn] = partitions match {
//      case None => table.partitionKeys()
//      case Some(columns) => columns.map(column =>
//        GlueColumn.builder.name(column).build).asJava
//    }
//
//    val storageDescriptor = table.storageDescriptor().toBuilder.location(location).build()
//    val tableInput = TableInput.builder().
//      name(table.name()).
//      tableType(table.tableType()).
//      partitionKeys(partitionKs).
//      storageDescriptor(storageDescriptor).
//      build()
//    val updateTableRequest = UpdateTableRequest.builder().databaseName(tableDetails.db).tableInput(tableInput).build()
//    glueClient.updateTable(updateTableRequest)
//  }
//
//  def createStorageDescriptor(location:String, fields: Fields, fileFormat: GlueFileFormat):StorageDescriptor = {
//    StorageDescriptor.builder()
//      .location(location)
//      .columns(fieldInfoToGlueType(fields):_*)
//      .inputFormat(fileFormat.inputFormat)
//      .outputFormat(fileFormat.outputFormat)
//      .numberOfBuckets(fileFormat.numberOfBuckets)
//      .serdeInfo(
//        SerDeInfo.builder()
//          .serializationLibrary(fileFormat.serializationLibrary)
//          .parameters(Map[String,String]("serialization.format" -> fileFormat.serializationFormat).asJava)
//          .build()
//      )
//      .build()
//  }
//
//  def doesTableExist(tableDetails:TableDetails)(implicit glueClient:GlueClient):Try[Boolean] = {
//    val req = GetTableRequest.builder().databaseName(tableDetails.db).name(tableDetails.name).build()
//    Try { glueClient.getTable(req) }.map(_.table().name() == tableDetails.name).recover{
//      case _:EntityNotFoundException => false
//    }
//  }
//
//  def dropTable(tableDetails:TableDetails) (implicit glueClient:GlueClient):Try[Unit] = Try{
//    val deleteTableRequest: DeleteTableRequest = DeleteTableRequest.builder().databaseName(tableDetails.db).name(tableDetails.name).build()
//    glueClient.deleteTable(deleteTableRequest)
//  }
//
//  def alterTableAddField() (implicit glueClient:GlueClient):Try[Unit] = {
//    Try{
//      //Implementation here
//    }
//  }
//  def alterTableDropField() (implicit glueClient:GlueClient):Try[Unit] = {
//    Try{
//
//    }
//  }
//
//  def createGlueClient(region:String = "ap-south-1"):GlueClient = {
//    val regionO = Region.of(region)
//    GlueClient.builder().region( regionO ).build()
//  }
//}
