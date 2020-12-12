package com.github.aly8246.format.input;

import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ConditionResolver {
    public final String sourceText;
    public final String whereConditionRegex = "where=.*]";
    public final String selectFieldsRegex = "fields=\\[.*?\\]";

    public String extractWhereSql() {
        String tempString = sourceText;
        Matcher matcher = Pattern.compile(whereConditionRegex).matcher(tempString);
        if (matcher.find()) {
            tempString = matcher.group();
        }
        tempString = tempString.replace("[", "");
        tempString = tempString.replace("]", "");
        tempString = tempString.replace(":BIGINT", "");
        tempString = tempString.replace("_UTF-16LE", "");
        tempString = tempString.replaceAll(":.*\\(\\d+\\)", "");
        tempString = tempString.replaceAll("CHARACTER SET \"UTF-16LE\"", "");
        tempString = tempString.replaceAll("where=", "where ");
        return " "+tempString;
    }

    public String[] extractSelectFields() {
        String tempString = sourceText;
        Matcher matcher = Pattern.compile(selectFieldsRegex).matcher(tempString);
        if (matcher.find()) {
            tempString = matcher.group();
        }
        tempString = tempString.replace("fields=", "");
        tempString = tempString.replace("[", "");
        tempString = tempString.replace("]", "");
        return tempString.split(",");
    }

    public ConditionResolver(String sourceText) {
        this.sourceText = sourceText;
    }

    public static void main(String[] args) {
        String sourceText = "Source: JdbcTableSource(id, name, sex) -> SourceConversion(table=[default_catalog.default_database.dim_user, source: [JdbcTableSource(id, name, sex)]], fields=[id, name, sex]) -> Calc(select=[id, name, sex], where=[((id = 1:BIGINT) OR ((sex = _UTF-16LE'男':VARCHAR(2147483647) CHARACTER SET \"UTF-16LE\") AND (name LIKE _UTF-16LE'%小')))]) -> SinkConversionToRow -> Sink: Print to Std. Out";
        ConditionResolver conditionResolver = new ConditionResolver(sourceText);
        System.out.println(conditionResolver.extractWhereSql());
        System.out.println(Arrays.toString(conditionResolver.extractSelectFields()));
    }
}
