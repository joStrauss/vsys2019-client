import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import java.io.File
import java.util.*
import java.util.regex.Pattern


open class Application {

    companion object {

        @JvmStatic
        fun main(args: Array<String>) {

            if (args.isEmpty()) {
                println("Please provide a valid path to a File!")
                return
            }

            val file = File(args[0])
            var gKey = ""
            var gValue = ""

            val kafkaProducer = KafkaProducer<String, String>(makeProperties())

            file.forEachLine { line ->
                val matcher = genomeIdPattern.matcher(line)
                if (matcher.matches()) {

                    gKey = matcher.group(1)
                    println("Found GenomeId $gKey \n\n\n")
                } else if (line.matches(Regex(genomeSeqPatternString))) {
                    gValue = line
                    println("Found genome: $gValue")
                }

                if (gKey.isNotEmpty() && gValue.isNotEmpty()) {
                    send(kafkaProducer, gKey, gValue)
                    println("\n\n\nAdded genome!\n\n\n")
                }
            }

            kafkaProducer.close()
        }

        private fun send(kafkaProducer: KafkaProducer<String, String>, key: String, value: String) {
            val rec = ProducerRecord<String, String>("Test", key, value)
            kafkaProducer.send(rec)
        }

        private fun makeProperties(): Properties {


            return Properties().apply {
                this["bootstrap.servers"] = "10.244.0.7:9092"
                this["enable.auto.commit"] = "true"
                this["auto.commit.interval.ms"] = "1000"
                this["auto.offset.reset"] = "earliest"
                this["session.timeout.ms"] = "30000"
                this["key.deserializer"] = "org.apache.kafka.common.serialization.StringDeserializer"
                this["value.deserializer"] = "org.apache.kafka.common.serialization.StringDeserializer"
                this["key.serializer"] = "org.apache.kafka.common.serialization.StringSerializer"
                this["value.serializer"] = "org.apache.kafka.common.serialization.StringSerializer"
                this["security.protocol"] = "SSL"
            }
        }

        private const val genomeSeqPatternString = "[ACGT]{20,}"
        private const val genomeIdPatternString = "@((.+?-){3}.+?)\\s.*"
        private val genomeIdPattern = Pattern.compile(genomeIdPatternString)
    }
}
