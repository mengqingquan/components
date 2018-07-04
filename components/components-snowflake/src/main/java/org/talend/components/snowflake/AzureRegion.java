package org.talend.components.snowflake;

public enum AzureRegion {
    BRAZIL_SOUTH("brazil-south"),
    CANADA_CENTRAL("canada-central"),
    CANADA_EAST("canada-east"),
    CENTRAL_US("central-us"),
    EAST_US("east-us"),
    EAST_US_2("east-us-2"),
    NORTH_CENTRAL_US("north-central-us"),
    SOUTH_CENTRAL_US("south-central-us"),
    WEST_CENTRAL_US("west-central-us"),
    WEST_US("west-us"),
    WEST_US_2("west-us-2"),
    AUSTRALIA_CENTRAL("australia-central"),
    AUSTRALIA_CENTRAL_2("australia-central-2"),
    AUSTRALIA_EAST("australia-east"),
    AUSTRALIA_SOUTHEAST("australia-southeast"),
    CENTRAL_INDIA("central-india"),
    CHINA_EAST("china-east"),
    CHINA_EAST_2("china-east-2"),
    CHINA_NORTH("china-north"),
    CHINA_NORTH_2("china-north-2"),
    EAST_ASIA("east-asia"),
    JAPAN_EAST("japan-east"),
    JAPAN_WEST("japan-west"),
    KOREA_CENTRAL("korea-central"),
    KOREA_SOUTH("korea-south"),
    SOUTH_INDIA("south-india"),
    SOUTHEAST_ASIA("southeast-asia"),
    WEST_INDIA("west-india"),
    US_DOD_CENTRAL("us-dod-central"),
    US_DOD_EAST("us-dod-east"),
    US_GOV_ARIZONA("us-gov-arizona"),
    US_GOV_IOWA("us-gov-iowa"),
    US_GOV_TEXAS("us-gov-texas"),
    US_GOV_VIRGINIA("us-gov-virginia"),
    US_SEC_EAST("us-sec-east"),
    US_SEC_WEST("us-sec-west"),
    FRANCE_CENTRAL("france-central"),
    FRANCE_SOUTH("france-south"),
    GERMANY_CENTRAL("germany-central"),
    GERMANY_NORTH("germany-north"),
    GERMANY_NORTHEAST("germany-northeast"),
    GERMANY_WEST_CENTRAL("germany-west-central"),
    NORTH_EUROPE("north-europe"),
    NORWAY_EAST("norway-east"),
    NORWAY_WES("norway-wes"),
    SWITZERLAND_NORTH("switzerland-north"),
    SWITZERLAND_WEST("switzerland-west"),
    UK_SOUTH("uk-south"),
    UK_WEST("uk-west"),
    WEST_EUROPE("west-europe"),
    SOUTH_AFRICA_NORTH("south-africa-north"),
    SOUTH_AFRICA_WEST("south-africa-west"),
    UAE_CENTRAL("uae-central"),
    UAE_NORTH("uae-north");

    private final String region;

    AzureRegion(String region){
        this.region = region;
    }

    public String getRegion() {
        return region;
    }
}