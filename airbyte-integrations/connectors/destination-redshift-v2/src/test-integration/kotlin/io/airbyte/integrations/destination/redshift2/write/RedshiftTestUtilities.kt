/*
 * Copyright (c) 2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.destination.redshift2.write

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ArrayNode
import com.zaxxer.hikari.HikariDataSource
import io.airbyte.cdk.load.data.AirbyteType
import io.airbyte.cdk.load.data.AirbyteValue
import io.airbyte.cdk.load.data.ArrayValue
import io.airbyte.cdk.load.data.ObjectValue
import io.airbyte.cdk.load.data.StringValue
import io.airbyte.cdk.load.data.TimestampWithTimezoneValue
import io.airbyte.cdk.load.message.Meta
import io.airbyte.cdk.load.test.util.ConfigurationUpdater
import io.airbyte.cdk.load.test.util.DefaultNamespaceResult
import io.airbyte.cdk.load.test.util.DestinationCleaner
import io.airbyte.cdk.load.test.util.ExpectedRecordMapper
import io.airbyte.cdk.load.test.util.IntegrationTest
import io.airbyte.cdk.load.test.util.OutputRecord
import io.airbyte.cdk.load.util.Jsons
import io.airbyte.integrations.destination.redshift2.config.RedshiftConfiguration
import io.airbyte.integrations.destination.redshift2.config.RedshiftConfigurationFactory
import io.airbyte.integrations.destination.redshift2.config.RedshiftSpecification
import io.airbyte.integrations.destination.redshift2.connect.RedshiftConnect
import io.airbyte.protocol.models.v0.AirbyteRecordMessageMetaChange
import io.github.oshai.kotlinlogging.KotlinLogging
import java.nio.file.Files
import java.nio.file.Path
import java.time.ZoneOffset

private val logger = KotlinLogging.logger {}

/** Path to the S3 staging config secrets file used by acceptance tests. */
const val CONFIG_PATH = "secrets/config_staging.json"

/**
 * Redshift normalizes timestamptz values to UTC and doesn't preserve the original timezone offset.
 * It also does not support null bytes in varchar columns. This mapper converts expected
 * timestamp_with_timezone values to UTC and removes null characters for comparison.
 */
object RedshiftTimestampNormalizationMapper : ExpectedRecordMapper {
    override fun mapRecord(expectedRecord: OutputRecord, schema: AirbyteType): OutputRecord {
        val sanitized = removeNullCharacters(expectedRecord.data)
        val normalized = normalizeTimestampsToUtc(sanitized)
        return expectedRecord.copy(data = normalized as ObjectValue)
    }

    private fun removeNullCharacters(value: AirbyteValue): AirbyteValue =
        when (value) {
            is StringValue -> StringValue(value.value.replace("\u0000", ""))
            is ArrayValue -> ArrayValue(value.values.map { removeNullCharacters(it) })
            is ObjectValue ->
                ObjectValue(
                    value.values.mapValuesTo(linkedMapOf()) { (_, v) -> removeNullCharacters(v) },
                )
            else -> value
        }

    private fun normalizeTimestampsToUtc(value: AirbyteValue): AirbyteValue =
        when (value) {
            is TimestampWithTimezoneValue ->
                TimestampWithTimezoneValue(value.value.withOffsetSameInstant(ZoneOffset.UTC))
            is ArrayValue -> ArrayValue(value.values.map { normalizeTimestampsToUtc(it) })
            is ObjectValue ->
                ObjectValue(
                    value.values.mapValuesTo(linkedMapOf()) { (_, v) ->
                        normalizeTimestampsToUtc(v)
                    },
                )
            else -> value
        }
}

/**
 * Configuration updater for Redshift acceptance tests. Since we read config from secrets files
 * (real Redshift cluster), no placeholder replacement is needed. Only the default namespace
 * replacement is required for test isolation.
 */
class RedshiftConfigUpdater : ConfigurationUpdater {
    override fun update(config: String): String = config

    override fun setDefaultNamespace(
        config: String,
        defaultNamespace: String,
    ): DefaultNamespaceResult =
        DefaultNamespaceResult(
            updatedConfig = config.replace("\"public\"", "\"$defaultNamespace\""),
            actualDefaultNamespace = defaultNamespace,
        )
}

/**
 * Cleans up old test schemas from Redshift. Connects to the cluster, lists schemas matching the
 * test namespace pattern, and drops schemas older than the retention period.
 */
object RedshiftDataCleaner : DestinationCleaner {
    override fun cleanup() {
        val configPath = Path.of(CONFIG_PATH)
        if (!Files.exists(configPath)) {
            logger.warn { "Secrets file not found at $CONFIG_PATH, skipping cleanup" }
            return
        }

        val config = loadConfig()
        val dataSource = RedshiftConnect(config).createDataSource()
        dataSource.use { ds ->
            ds.connection.use { connection ->
                val statement = connection.createStatement()
                val schemas =
                    statement.executeQuery(
                        """
                        SELECT schema_name
                        FROM information_schema.schemata
                        WHERE schema_name LIKE 'test%'
                        """.trimIndent(),
                    )
                while (schemas.next()) {
                    val schemaName = schemas.getString("schema_name")
                    if (IntegrationTest.isNamespaceOld(schemaName)) {
                        logger.info { "Dropping old test schema: $schemaName" }
                        statement.execute("DROP SCHEMA IF EXISTS \"$schemaName\" CASCADE")
                    }
                }
            }
        }
    }
}

/** Parses the `_airbyte_meta` SUPER column JSON into an [OutputRecord.Meta] object. */
fun stringToMeta(metaAsString: String?): OutputRecord.Meta? {
    if (metaAsString.isNullOrEmpty()) {
        return null
    }
    val metaJson = Jsons.readTree(metaAsString)

    val changes =
        (metaJson["changes"] as ArrayNode).map { change ->
            val changeNode = change as JsonNode
            Meta.Change(
                field = changeNode["field"].textValue(),
                change =
                    AirbyteRecordMessageMetaChange.Change.fromValue(
                        changeNode["change"].textValue(),
                    ),
                reason =
                    AirbyteRecordMessageMetaChange.Reason.fromValue(
                        changeNode["reason"].textValue(),
                    ),
            )
        }

    return OutputRecord.Meta(changes = changes, syncId = metaJson["sync_id"].longValue())
}

/** Loads a [RedshiftConfiguration] from the secrets file. */
fun loadConfig(): RedshiftConfiguration {
    val configJson = Files.readString(Path.of(CONFIG_PATH))
    val mapper =
        com.fasterxml.jackson.databind
            .ObjectMapper()
            .configure(
                com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES,
                false,
            )
    val spec = mapper.readValue(configJson, RedshiftSpecification::class.java)
    return RedshiftConfigurationFactory().makeWithoutExceptionHandling(spec)
}

/**
 * Creates a [HikariDataSource] from a [RedshiftConfiguration]. Convenience function used by
 * [RedshiftDataDumper] and other test utilities.
 */
fun createDataSource(config: RedshiftConfiguration): HikariDataSource {
    return RedshiftConnect(config).createDataSource()
}
