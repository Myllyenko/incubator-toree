/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License
 */

package org.apache.toree.kernel.interpreter.scala

import java.io.{ByteArrayOutputStream, PrintStream}
import java.net.{URL, URLClassLoader}
import java.nio.charset.Charset
import java.util.concurrent.ExecutionException

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.SparkContext
import org.apache.spark.repl.{SparkIMain, SparkJLineCompletion}
import org.apache.spark.sql.SQLContext
import org.apache.toree.interpreter._
import org.apache.toree.kernel.api.{KernelLike, KernelOptions}
import org.apache.toree.utils.{MultiOutputStream, SparkUtils, TaskManager}
import org.slf4j.LoggerFactory
import org.apache.toree.kernel.BuildInfo

import scala.annotation.tailrec
import scala.concurrent.{Await, Future}
import scala.language.reflectiveCalls
import scala.tools.nsc.interpreter.{IR, JPrintWriter, OutputStream}
import scala.tools.nsc.util.ClassPath
import scala.tools.nsc.{Settings, io}
import scala.util.{Try => UtilTry}

class ScalaInterpreter(private val config:Config = ConfigFactory.load) extends Interpreter with ScalaInterpreterSpecific {
  protected val logger = LoggerFactory.getLogger(this.getClass.getName)

  protected val _thisClassloader = this.getClass.getClassLoader

  protected val _runtimeClassloader =
    new URLClassLoader(Array(), _thisClassloader) {
      def addJar(url: URL) = this.addURL(url)
    }
  protected val lastResultOut = new ByteArrayOutputStream()


  protected val multiOutputStream = MultiOutputStream(List(Console.out, lastResultOut))
  private[scala] var taskManager: TaskManager = _

  protected var settings: Settings = newSettings(interpreterArgs())

  settings.classpath.value = buildClasspath(_thisClassloader)
  settings.embeddedDefaults(_runtimeClassloader)

  private val maxInterpreterThreads: Int = {
    if(config.hasPath("max_interpreter_threads"))
      config.getInt("max_interpreter_threads")
    else
      TaskManager.DefaultMaximumWorkers
  }

  protected def newTaskManager(): TaskManager =
    new TaskManager(maximumWorkers = maxInterpreterThreads)

  protected def refreshDefinitions(): Unit = {

    /*
     * Refreshing values, avoids refreshing any defs
     */
    sparkIMain.definedTerms.map(_.toString).filterNot(sparkIMain.symbolOfTerm(_).isSourceMethod).foreach(termName => {
      val termTypeString = sparkIMain.typeOfTerm(termName).toLongString
      sparkIMain.valueOfTerm(termName) match {
        case Some(termValue)  =>
          val modifiers = buildModifierList(termName)
          logger.debug(s"Rebinding of $termName as " +
            s"${modifiers.mkString(" ")} $termTypeString")
          UtilTry(sparkIMain.beSilentDuring {
            sparkIMain.bind(
              termName, termTypeString, termValue, modifiers
            )
          })
        case None             =>
          logger.debug(s"Ignoring rebinding of $termName")
      }
    })

    /*
     * Need to rebind the defs for sc and sqlContext
     */
    bindSparkContext()
    bindSqlContext()
  }

  override def init(kernel: KernelLike): Interpreter = {
    start()
    bindKernelVariable(kernel)

    // ensure bindings are defined before allowing user code to run
    bindSqlContext()
    bindSparkContext()
    defineImplicits()

    this
  }

  protected[scala] def buildClasspath(classLoader: ClassLoader): String = {

    def toClassLoaderList( classLoader: ClassLoader ): Seq[ClassLoader] = {
      @tailrec
      def toClassLoaderListHelper( aClassLoader: ClassLoader, theList: Seq[ClassLoader]):Seq[ClassLoader] = {
        if( aClassLoader == null )
          return theList

        toClassLoaderListHelper( aClassLoader.getParent, aClassLoader +: theList )
      }
      toClassLoaderListHelper(classLoader, Seq())
    }

    val urls = toClassLoaderList(classLoader).flatMap{
        case cl: java.net.URLClassLoader => cl.getURLs.toList
        case a => List()
    }

    urls.foldLeft("")((l, r) => ClassPath.join(l, r.toString))
  }

  protected def interpreterArgs(): List[String] = {
    import scala.collection.JavaConverters._
    config.getStringList("interpreter_args").asScala.toList
  }

  protected def bindKernelVariable(kernel: KernelLike): Unit = {
    doQuietly {
      bind(
        "kernel", "org.apache.toree.kernel.api.Kernel",
        kernel, List( """@transient implicit""")
      )
    }
  }

  override def interrupt(): Interpreter = {
    require(sparkIMain != null && taskManager != null)

    // Force dumping of current task (begin processing new tasks)
    taskManager.restart()

    this
  }

  override def interpret(code: String, silent: Boolean = false, output: Option[OutputStream]):
  (Results.Result, Either[ExecuteOutput, ExecuteFailure]) = {
    val starting = (Results.Success, Left(""))
    interpretRec(code.trim.split("\n").toList, false, starting)
  }

  def truncateResult(result:String, showType:Boolean =false, noTruncate: Boolean = false): String = {
    val resultRX="""(?s)(res\d+):\s+(.+)\s+=\s+(.*)""".r

    result match {
      case resultRX(varName,varType,resString) => {
          var returnStr=resString
          if (noTruncate)
          {
            val r=read(varName)
            returnStr=r.getOrElse("").toString
          }

          if (showType)
            returnStr=varType+" = "+returnStr

        returnStr

      }
      case _ => ""
    }


  }

  protected def interpretRec(lines: List[String], silent: Boolean = false, results: (Results.Result, Either[ExecuteOutput, ExecuteFailure])): (Results.Result, Either[ExecuteOutput, ExecuteFailure]) = {
    lines match {
      case Nil => results
      case x :: xs =>
        val output = interpretLine(x)

        output._1 match {
          // if success, keep interpreting and aggregate ExecuteOutputs
          case Results.Success =>
            val result = for {
              originalResult <- output._2.left
            } yield(truncateResult(originalResult, KernelOptions.showTypes,KernelOptions.noTruncation))
            interpretRec(xs, silent, (output._1, result))

          // if incomplete, keep combining incomplete statements
          case Results.Incomplete =>
            xs match {
              case Nil => interpretRec(Nil, silent, (Results.Incomplete, results._2))
              case _ => interpretRec(x + "\n" + xs.head :: xs.tail, silent, results)
            }

          //
          case Results.Aborted =>
            output
             //interpretRec(Nil, silent, output)

          // if failure, stop interpreting and return the error
          case Results.Error =>
            val result = for {
              curr <- output._2.right
            } yield curr
            interpretRec(Nil, silent, (output._1, result))
        }
    }
  }


  protected def interpretLine(line: String, silent: Boolean = false):
    (Results.Result, Either[ExecuteOutput, ExecuteFailure]) =
  {
    require(sparkIMain != null && taskManager != null)
    logger.trace(s"Interpreting line: $line")

    val futureResult = interpretAddTask(line, silent)

    // Map the old result types to our new types
    val mappedFutureResult = interpretMapToCustomResult(futureResult)

    // Determine whether to provide an error or output
    val futureResultAndOutput = interpretMapToResultAndOutput(mappedFutureResult)

    val futureResultAndExecuteInfo =
      interpretMapToResultAndExecuteInfo(futureResultAndOutput)

    // Block indefinitely until our result has arrived
    import scala.concurrent.duration._
    Await.result(futureResultAndExecuteInfo, Duration.Inf)
  }

  protected def interpretMapToCustomResult(future: Future[IR.Result]) = {
    import scala.concurrent.ExecutionContext.Implicits.global
    future map {
      case IR.Success             => Results.Success
      case IR.Error               => Results.Error
      case IR.Incomplete          => Results.Incomplete
    } recover {
      case ex: ExecutionException => Results.Aborted
    }
  }

  protected def interpretMapToResultAndOutput(future: Future[Results.Result]) = {
    import scala.concurrent.ExecutionContext.Implicits.global
    future map {
      result =>
        val output =
          lastResultOut.toString(Charset.forName("UTF-8").name()).trim
        lastResultOut.reset()
        (result, output)
    }
  }


  override def start() = {
    require(sparkIMain == null && taskManager == null)

    taskManager = newTaskManager()

    logger.debug("Initializing task manager")
    taskManager.start()

    sparkIMain =
      newSparkIMain(settings, new JPrintWriter(multiOutputStream, true))


    //logger.debug("Initializing interpreter")
    //sparkIMain.initializeSynchronous()

    logger.debug("Initializing completer")
    jLineCompleter = new SparkJLineCompletion(sparkIMain)

    sparkIMain.beQuietDuring {
      //logger.info("Rerouting Console and System related input and output")
      //updatePrintStreams(System.in, multiOutputStream, multiOutputStream)

//   ADD IMPORTS generates too many classes, client is responsible for adding import
      logger.debug("Adding org.apache.spark.SparkContext._ to imports")
      sparkIMain.addImports("org.apache.spark.SparkContext._")
    }

    this
  }

  def bindSparkContext() = {
    val bindName = "sc"

    doQuietly {
      logger.info(s"Binding SparkContext into interpreter as $bindName")
      interpret(s"""def ${bindName}: ${classOf[SparkContext].getName} = kernel.sparkContext""")

      // NOTE: This is needed because interpreter blows up after adding
      //       dependencies to SparkContext and Interpreter before the
      //       cluster has been used... not exactly sure why this is the case
      // TODO: Investigate why the cluster has to be initialized in the kernel
      //       to avoid the kernel's interpreter blowing up (must be done
      //       inside the interpreter)
      logger.debug("Initializing Spark cluster in interpreter")

      doQuietly {
        interpret(Seq(
          "val $toBeNulled = {",
          "  var $toBeNulled = sc.emptyRDD.collect()",
          "  $toBeNulled = null",
          "}"
        ).mkString("\n").trim())
      }
    }
  }

  def bindSqlContext(): Unit = {
    val bindName = "sqlContext"

    doQuietly {
      // TODO: This only adds the context to the main interpreter AND
      //       is limited to the Scala interpreter interface
      logger.debug(s"Binding SQLContext into interpreter as $bindName")

      interpret(s"""def ${bindName}: ${classOf[SQLContext].getName} = kernel.sqlContext""")
    }
  }

  def defineImplicits(): Unit = {
    val code =
      """
        |import org.apache.spark.SparkContext
        |import org.apache.spark.sql.SQLContext
        |import org.apache.spark.sql.SQLImplicits
        |
        |object implicits extends SQLImplicits with Serializable {
        |  protected override def _sqlContext: SQLContext = SQLContext.getOrCreate(sc)
        |}
        |
        |import implicits._
      """.stripMargin
    doQuietly(interpret(code))
  }


  override def classLoader: ClassLoader = _runtimeClassloader

  /**
    * Returns the language metadata for syntax highlighting
    */
  override def languageInfo = LanguageInfo(
    "scala", BuildInfo.scalaVersion,
    fileExtension = Some(".scala"),
    pygmentsLexer = Some("scala"),
    mimeType = Some("text/x-scala"),
    codemirrorMode = Some("text/x-scala"))
}

object ScalaInterpreter {

  /**
    * Utility method to ensure that a temporary directory for the REPL exists for testing purposes.
    */
  def ensureTemporaryFolder(): String = {
    val outputDir = Option(System.getProperty("spark.repl.class.outputDir")).getOrElse({

      val execUri = System.getenv("SPARK_EXECUTOR_URI")
      val tmp = System.getProperty("java.io.tmpdir")
      val rootDir = System.getProperty("spark.repl.classdir", tmp)
      val outputDir: String = SparkUtils.createTempDir(rootDir).getAbsolutePath
      System.setProperty("spark.repl.class.outputDir", outputDir)
      if (execUri != null) {
        System.setProperty("spark.executor.uri", execUri)
      }
      outputDir
    })
    outputDir
  }

}
