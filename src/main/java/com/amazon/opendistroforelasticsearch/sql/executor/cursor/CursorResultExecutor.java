/*
 *   Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License").
 *   You may not use this file except in compliance with the License.
 *   A copy of the License is located at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   or in the "license" file accompanying this file. This file is distributed
 *   on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *   express or implied. See the License for the specific language governing
 *   permissions and limitations under the License.
 */

package com.amazon.opendistroforelasticsearch.sql.executor.cursor;

import com.amazon.opendistroforelasticsearch.sql.executor.Format;
import com.amazon.opendistroforelasticsearch.sql.executor.format.Protocol;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.search.ClearScrollResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.search.SearchHits;
import org.json.JSONObject;

import java.util.Base64;
import java.util.Map;

import static org.elasticsearch.rest.RestStatus.OK;

public class CursorResultExecutor implements CursorRestExecutor {

    public static final int SCROLL_TIMEOUT = 12000; // 2 minutes

    private String cursorId;
    private Format format;

    private static final Logger LOG = LogManager.getLogger(CursorResultExecutor.class);

    public CursorResultExecutor(String cursorId, Format format) {
        this.cursorId = cursorId;
        this.format = format;
    }

    public void execute(Client client, Map<String, String> params, RestChannel channel) throws Exception {
        LOG.info("executing something inside CursorResultExecutor execute");
        String formattedResponse = execute(client, params);
//        LOG.info("{} : {}", cursorId, formattedResponse);
        channel.sendResponse(new BytesRestResponse(OK, "application/json; charset=UTF-8", formattedResponse));
    }

    public String execute(Client client, Map<String, String> params) throws Exception {
        // TODO: throw correct Exception , use try catch if needed
        String decodedCursorContext = new String(Base64.getDecoder().decode(cursorId));
        JSONObject cursorJson = new JSONObject(decodedCursorContext);

        String type = cursorJson.optString("type", null); // see if it is a good case to use Optionals
        CursorType cursorType = null;

        if (type != null) {
            cursorType = CursorType.valueOf(type);
        }

        if (cursorType!=null) {
            switch(cursorType) {
                case DEFAULT:
                    return handleDefaultCursorRequest(client, cursorJson);
                case AGGREGATION:
                    return handleAggregationCursorRequest(client, cursorJson);
                case JOIN:
                    return handleJoinCursorRequest(client, cursorJson);
                default: throw new ElasticsearchException("Invalid cursor Id");
            }
        }
        // got this from elasticsearch when "Cannot parse scroll id" when passed a wrong scrollid
        throw new ElasticsearchException("Invalid cursor Id");
    }

    private String handleDefaultCursorRequest(Client client, JSONObject cursorContext) {
        //validate jsonobject for all the needed fields
        LOG.info("Inside handleDefaultCursorRequest");
        String previousScrollId = cursorContext.getString("scrollId");
        SearchResponse scrollResponse = client.prepareSearchScroll(previousScrollId).
                setScroll(TimeValue.timeValueSeconds(SCROLL_TIMEOUT)).get();
        SearchHits searchHits = scrollResponse.getHits();
        String newScrollId = scrollResponse.getScrollId();

        int pagesLeft = cursorContext.getInt("left");
        pagesLeft--;

        if (pagesLeft <=0) {
            // TODO : close the cursor on the last page
            LOG.info("Closing the cursor as size is {}", pagesLeft);
            ClearScrollResponse clearScrollResponse = client.prepareClearScroll().addScrollId(newScrollId).get();

            if (!clearScrollResponse.isSucceeded()) {
                LOG.info("Problem closing the cursor context {} ", newScrollId);
            }

            Protocol protocol = new Protocol(client, searchHits, cursorContext, format.name().toLowerCase());
            protocol.setCursor(null);
            return protocol.cursorFormat();

        } else {
            LOG.info("Generating next page, pagesLeft {}", pagesLeft);
            cursorContext.put("left", pagesLeft);
            cursorContext.put("scrollId", newScrollId);
            LOG.info("New scroll ID {}", newScrollId);
            Protocol protocol = new Protocol(client, searchHits, cursorContext, format.name().toLowerCase());
            LOG.info("cursorContext before encoding {}", cursorContext);
            String cursorId = protocol.encodeCursorContext(cursorContext);
            LOG.info("New cursor ID {}", cursorId);
            protocol.setCursor(cursorId);
            LOG.info("Set cursor to protocol {}", cursorId);
            return protocol.cursorFormat();
        }
    }

    private String handleAggregationCursorRequest(Client client, JSONObject cursorContext) {
        return "something";
    }

    private String handleJoinCursorRequest(Client client, JSONObject cursorContext) {
        return "something";
    }

}