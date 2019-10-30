import java.io.{File, FileOutputStream, OutputStreamWriter}

object FileUtils {

  def writeToTempFile(contents: String, filePrefix: String, fileSuffix: String, charset: String = "UTF-8"): String = {
    val tempFile = File.createTempFile(filePrefix, fileSuffix)
    val tempWriter = new OutputStreamWriter(new FileOutputStream(tempFile), charset)
    tempWriter.write(contents)
    tempWriter.close()
    tempFile.getAbsolutePath
  }

}
