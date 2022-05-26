package com.homeaway.datatools.photon.utils;

import org.slf4j.Logger;

import java.util.function.Function;

public class EnvironmentVariablesUtils {
    public static <T> T getEnvVarOrUseDefault(
            final String envVarName,
            T defaultValue,
            Logger log,
            Function<String, T> parsingFunction
    ) {
        T result;
        final String envVarValue = System.getenv(envVarName);
        if (envVarValue == null) {
            log.info(
                    "Environment variable {} is not set. Will use the default {} value.",
                    envVarName,
                    defaultValue
            );
            result = defaultValue;
        } else {
            log.info(
                    "Environment variable {} is set to {}",
                    envVarName,
                    envVarValue
            );
            result = parsingFunction.apply(envVarValue);
        }
        return result;
    }

    public static long getLongOrUseDefault(final String envVarName, Long defaultValue, Logger log) {
        Function<String, Long> parsingFunction = (String arg) -> parseLong(envVarName, arg);
        return getEnvVarOrUseDefault(envVarName, defaultValue, log, parsingFunction);
    }

    public static long parseLong(final String envVarName, final String envVarValue) {
        long result;
        try {
            result = Long.parseLong(envVarValue);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                    String.format(
                            "Failed to parse value %s of environment variable %s as a Long value.",
                            envVarValue,
                            envVarName
                    ),
                    e
            );
        }
        return result;
    }
}
