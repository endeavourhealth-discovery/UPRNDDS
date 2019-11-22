package org.endeavourhealth.RALF;

import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.Properties;

public class RalfRunner {

    public static void main(String... args) throws IOException, SQLException {

        Properties properties = loadProperties( args );

        try (  generate RalfExporter = new generate( properties  ) ) {
            RalfExporter.export();

        } catch (Exception e) {
            System.out.println(e);
        }
    }

    private static Properties loadProperties(String[] args) throws IOException {

        Properties properties = new Properties();

        InputStream inputStream = RalfRunner.class.getClassLoader().getResourceAsStream("ralf.exporter.properties");

        properties.load( inputStream );

        return properties;
    }

}

