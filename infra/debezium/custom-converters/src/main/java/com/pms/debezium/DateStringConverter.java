package com.pms.debezium;

import io.debezium.spi.converter.CustomConverter;
import io.debezium.spi.converter.RelationalColumn;
import org.apache.kafka.connect.data.SchemaBuilder;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Properties;

/**
 * Emits PostgreSQL DATE columns as plain "yyyy-MM-dd" strings instead of
 * Debezium's default io.debezium.time.Date logical type (an epoch-day
 * integer), so payment_lines' billing_period_start/end and due_date/
 * payment_date conform to payment_line.v1.json's "format": "date" contract.
 *
 * Kafka Connect's bundled TimestampConverter SMT cannot do this - it only
 * recognizes Kafka Connect's own logical type names (org.apache.kafka.connect.data.Date),
 * not Debezium's differently-named one, and fails the task if you try
 * (see error-handling/debezium-date-epoch-day-encoding.md). This is Debezium's
 * own documented extension point for exactly this situation.
 */
public class DateStringConverter implements CustomConverter<SchemaBuilder, RelationalColumn> {

    private DateTimeFormatter dateFormatter;

    @Override
    public void configure(Properties props) {
        String pattern = props.getProperty("format.date", "yyyy-MM-dd");
        this.dateFormatter = DateTimeFormatter.ofPattern(pattern);
    }

    @Override
    public void converterFor(RelationalColumn column, ConverterRegistration<SchemaBuilder> registration) {
        if (!"date".equalsIgnoreCase(column.typeName())) {
            return;
        }

        registration.register(SchemaBuilder.string().optional(), rawValue -> {
            if (rawValue == null) {
                return null;
            }
            if (rawValue instanceof LocalDate) {
                return ((LocalDate) rawValue).format(dateFormatter);
            }
            if (rawValue instanceof Number) {
                // Fallback: some connector paths hand back epoch-day integers directly.
                return LocalDate.ofEpochDay(((Number) rawValue).longValue()).format(dateFormatter);
            }
            return rawValue.toString();
        });
    }
}
