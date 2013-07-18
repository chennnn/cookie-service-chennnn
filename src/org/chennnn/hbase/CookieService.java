package org.chennnn.hbase;

import org.apache.hadoop.hbase.util.Bytes;
import org.chennnn.hbase.HBaseBasic;

public class CookieService {
	public static final String TAG_MEDIA = "m";
	public static final String TAG_CAMPAIGN = "c";
	public static final String TAG_PLACEMENT = "p";
	public static final String TAG_GLOBAL_MEDIA = "g";
	public static final String TAG_KEYWORD = "k";
	public static final String TAG_REGION = "r";

	public static String reverseString(String s) {
		StringBuilder sb = new StringBuilder();
		int length = s.length();
		for (int i = length - 1; i >= 0; i--) {
			sb.append(s.charAt(i));
		}
		return sb.toString();
	}

	public static String makeRowKey(String cookieId, int timeStamp) {
		char baseTime = (char) (timeStamp >> 16);
		String reversedCookieID = reverseString(cookieId);
		byte[] bytesCookieId = Bytes.toBytes(reversedCookieID);
		byte[] bytesBaseTime = Bytes.toBytes((short) baseTime);
		byte[] result = new byte[bytesCookieId.length + bytesBaseTime.length];
		System.arraycopy(bytesCookieId, 0, result, 0, bytesCookieId.length);
		System.arraycopy(bytesBaseTime, 0, result, bytesCookieId.length,
				bytesBaseTime.length);
		return Bytes.toString(result);
	}

	public static String makeQualifier(String type, int timeStamp) {
		byte[] qualifier = new byte[1 + Character.SIZE / 8];
		short deltaTime = (short) (char) (timeStamp & 0xFFFF);
		System.arraycopy(Bytes.toBytes(type), 0, qualifier, 0, type.length());
		System.arraycopy(Bytes.toBytes(deltaTime), 0, qualifier, type.length(),
				Character.SIZE / 8);
		return Bytes.toString(qualifier);
	}

	public static void insertCookies(String cookieString, String tableName)
			throws Exception {
		String str[] = cookieString.split(",");
		String cookieId = str[0];
		int timeStamp = Integer.parseInt(str[12]);
		String media = str[22];
		String globalMedia = str[44];
		String camping = str[17];
		String placement = str[25];
		String keyWord = str[43];
		String region = str[39] + str[40];

		String rowKey = makeRowKey(cookieId, timeStamp);
		HBaseBasic.addRecord(tableName, rowKey, "t",
				makeQualifier(TAG_MEDIA, timeStamp), media);
		HBaseBasic.addRecord(tableName, rowKey, "t",
				makeQualifier(TAG_GLOBAL_MEDIA, timeStamp), globalMedia);
		HBaseBasic.addRecord(tableName, rowKey, "t",
				makeQualifier(TAG_CAMPAIGN, timeStamp), camping);
		HBaseBasic.addRecord(tableName, rowKey, "t",
				makeQualifier(TAG_PLACEMENT, timeStamp), placement);
		HBaseBasic.addRecord(tableName, rowKey, "t",
				makeQualifier(TAG_REGION, timeStamp), region);
		HBaseBasic.addRecord(tableName, rowKey, "t",
				makeQualifier(TAG_KEYWORD, timeStamp), keyWord);
	}

	public static void main(String[] agrs) throws Exception {
		String tablename = "cookieService";// 表明
		String[] familys = "t".split(" ");

		HBaseBasic.creatTable(tablename, familys);
		//insertCookies(cookieString, tableName)
		insertCookies(
				"1304110020521752640,3059244346,0000019595900,13,1304,1316,130421,13042100,04,16,21,00,1366473601,,,,202,13240,,,,0,1897,,,200169913,,,,,,,,,,,,,,45,0771,1,,,,1304110020521752640,,,,,,http%3A//www.iqiyi.com/player/20130419210139/Player.swf/%5B%5BDYNAMIC%5D%5D/2,1,%3Fv%3Dip%3A182.88.93.58,http%3A//www.iqiyi.com/player/20130419210139/Player.swf/%5B%5BDYNAMIC%5D%5D/2",
				tablename);
		insertCookies(
				"1302210100151876041,1959227576,0000038730596,13,1304,1316,130421,13042100,04,16,21,00,1366473603,,,,202,13240,,,,0,1897,,,200169913,,,,,,,,,,,,,,44,0020,1,,,,1302210100151876041,,,,,,http%3A//www.iqiyi.com/player/20130419210139/Player.swf,,%3Fv%3Dip%3A116.199.112.184,http%3A//www.iqiyi.com/player/20130419210139/Player.swf",
				tablename);
		insertCookies(
				"1302210100151876041,1959227576,0000038730596,13,1304,1316,130421,13042100,04,16,21,00,1366473603,,,,202,13240,,,,0,1897,,,200169913,,,,,,,,,,,,,,44,0020,1,,,,1302210100151876041,,,,,,http%3A//www.iqiyi.com/player/20130419210139/Player.swf,,%3Fv%3Dip%3A116.199.112.184,http%3A//www.iqiyi.com/player/20130419210139/Player.swf",
				tablename);
	}
}
