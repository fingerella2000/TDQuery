package com.td.toolkit.utils;


import java.util.ArrayList;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An extended parser limited the following matching mechanisms the DefaultParser provided:
 *
 * 1. Treat option as invalid if it partially match the predefined options.
 *
 * For example: -columnx option may match the short option -c and taking "olumnx" as option value.
 *
 * 2. Treat short option as invalid if it matches the long predefined options.
 *
 * For example: -column option may match the long option --column.
 *
 * @author  Bryan Luo
 * @version 1.0
 * @since   1.0
 *
 */
public class ExactMatchParser extends DefaultParser {

    final static Logger logger = LoggerFactory.getLogger(ExactMatchParser.class);

    private String stripLeadingHyphens(String str) {
        if (str == null) {
            return null;
        } else if (str.startsWith("--")) {
            return str.substring(2, str.length());
        } else {
            return str.startsWith("-") ? str.substring(1, str.length()) : str;
        }
    }

    @Override
    public CommandLine parse(Options options, String[] arguments) throws ParseException {
        List<String> knownArguments = new ArrayList<>();
        List<String> rawArguments = new ArrayList<>();

        boolean is_option_arg = false;
        int args_length = arguments.length;

        for(int i = 0; i < args_length; ++i) {
            String arg = arguments[i];
            if (arg.startsWith("-") && !"-".equals(arg)) { // parser encountered an option
                String t = stripLeadingHyphens(arg);
                if (arg.startsWith("--")) { // option with long name
                    if (options.hasLongOption(t)) {
                        knownArguments.add(arg);
                        logger.debug("Parsed an valid option: {}", t);
                    }else {
                        throw new ParseException("Unrecognized option: " + arg);
                    }
                }else if (arg.startsWith("-")) { // option with short name
                    if (options.hasShortOption(t)) {
                        knownArguments.add(arg);
                        logger.debug("Parsed an valid option: {}", t);
                    }else {
                        throw new ParseException("Unrecognized option: " + arg);
                    }
                }
                    is_option_arg = true;
            }else if (is_option_arg) { // parser encountered an option argument
                knownArguments.add(arg);
                logger.debug("Parsed an valid option argument: {}", arg);
                is_option_arg = false;
            }else { // parser encounter an normal argument
                rawArguments.add(arg);
                logger.debug("Parsed an normal argument: {}", arg);
            }
        }
        knownArguments.addAll(rawArguments);
        return super.parse(options, knownArguments.toArray(new String[knownArguments.size()]));
    }
}

