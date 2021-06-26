package com.praveenray.quarkus.services

import io.smallrye.mutiny.Multi
import io.vertx.core.file.OpenOptions
import io.vertx.mutiny.core.Vertx
import io.vertx.mutiny.core.buffer.Buffer
import org.apache.commons.csv.CSVFormat
import org.jboss.logging.Logger
import java.io.StringReader
import java.nio.file.Path
import java.util.concurrent.atomic.AtomicReference
import javax.enterprise.context.ApplicationScoped

@ApplicationScoped
class CSVToKafkaService(
    private val vertx: Vertx,
) {
    private val logger = Logger.getLogger(CSVToKafkaService::class.java)
    
    fun readAccountsFile(path: Path): Multi<Map<String, Any>> {
        val uni = vertx.fileSystem().open(path.toAbsolutePath().toString(), OpenOptions().setRead(true))
        var intermediateBuffer = AtomicReference(Buffer.buffer())
        val header = AtomicReference<List<String>>(null)
        return uni.onItem().transformToMulti { file ->
            file.setReadBufferSize(10 * 1024)
            file.toMulti()
        }.onItem().transformToMultiAndConcatenate { buffer ->
            val (listOfBuffers, lastBuffer) = splitIntoLines(intermediateBuffer.get().appendBuffer(buffer), emptyList())
            intermediateBuffer.set(lastBuffer)
            Multi.createFrom().items(*listOfBuffers.toTypedArray())
        }.map {
            val str = it.toString()
            logger.info("LINE: $str")
            val csvParser = CSVFormat.DEFAULT.parse(StringReader(str))
            val records = csvParser.records
            if (records.isNotEmpty()) {
                val record = records.first()
                val fields = record.toList()
                if (!header.compareAndSet(null, fields)) {
                    val headerFields = header.get()
                    fields.foldIndexed(emptyMap<String, Any>()) { index, map, field ->
                        map.plus(headerFields[index] to field)
                    }
                } else emptyMap()
            } else emptyMap()
        }.filter { it.isNotEmpty() }
    }
    
    private fun splitIntoLines(buffer: Buffer, accumulator: List<Buffer>): Pair<List<Buffer>, Buffer> {
        val newlines = listOf(0x0d, 0x0a)
        val index = buffer.bytes.indexOfFirst { it.toInt() in newlines }
        return when {
            index >= 0 -> {
                val accumulated = accumulator.plus(buffer.getBuffer(0, index))
                splitIntoLines(buffer.getBuffer(index + 1, buffer.length()), accumulated)
            }
            buffer.length() > 0 -> {
                Pair(accumulator, buffer)
            }
            else -> Pair(accumulator, Buffer.buffer())
        }
    }
}