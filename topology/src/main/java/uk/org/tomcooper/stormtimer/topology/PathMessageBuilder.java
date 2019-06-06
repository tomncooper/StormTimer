package uk.org.tomcooper.stormtimer.topology;

import org.apache.storm.tuple.Tuple;

import com.google.gson.Gson;

class PathMessageBuilder {

	public static String createPathMessageStr(String name, int taskID, Tuple input) {
		String path1 = input.getStringByField("path");
		String path2 = name + ":" + taskID;
		String[] path = { path1, path2 };
		String messageID = input.getStringByField("uuid");
		long originTimestamp = input.getLongByField("messageTimestamp");

		PathMessage pathMsg = new PathMessage();
		pathMsg.setMessageID(messageID);
		pathMsg.setOriginTimestamp(originTimestamp);
		pathMsg.setPath(path);
		
		Gson gson = new Gson();
		String pathMessage = gson.toJson(pathMsg);

		return pathMessage;
	}
}
