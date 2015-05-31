package com.emc.data.transformer;

import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.integration.annotation.MessageEndpoint;
import org.springframework.integration.annotation.Transformer;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * simple flattener of twitter data
 * @author willschipp
 *
 */
@MessageEndpoint
public class TwitterFlattener {

	private static final String DELIMITER = "|";
	private static final String ARRAY_DELIMITER = ",";
	
	@Autowired
	private ObjectMapper mapper;
	
	@Transformer(inputChannel="input",outputChannel="output")
	public String flattenTwitter(String tweet) throws Exception {
		StringBuffer result = new StringBuffer();
		//convert the tweet to a map
		Map<String,Object> map = mapper.readValue(tweet, new TypeReference<Map<String,Object>>() {});
		//now extract what's necessary
		result.append(map.get("id_str"));
		result.append(DELIMITER);
		result.append(map.get("created_at"));
		result.append(DELIMITER);
		result.append(map.get("in_reply_to_status_id_str"));
		result.append(DELIMITER);
		result.append(map.get("lang"));
		result.append(DELIMITER);		
		result.append(map.get("source"));
		result.append(DELIMITER);
		result.append(map.get("text"));
		result.append(DELIMITER);
		result.append(((Map<String,Object>)map.get("user")).get("created_at"));
		result.append(DELIMITER);
		result.append(((Map<String,Object>)map.get("user")).get("id_str"));
		result.append(DELIMITER);
		result.append(((Map<String,Object>)map.get("user")).get("name"));
		result.append(DELIMITER);
		result.append(((Map<String,Object>)map.get("user")).get("screen_name"));
		result.append(DELIMITER);		
		result.append(((Map<String,Object>)map.get("user")).get("location"));
		result.append(DELIMITER);
		//process hashtags
		if (!((List<Map<String,Object>>)((Map<String,Object>)map.get("entities")).get("hashtags")).isEmpty()) {
			StringBuffer hashtags = new StringBuffer();
			//process
			for (Map<String,Object> hashtag: (List<Map<String,Object>>)((Map<String,Object>)map.get("entities")).get("hashtags")) {
				if (hashtags.length() > 0) {
					hashtags.append(ARRAY_DELIMITER);
				}//end if
				hashtags.append(hashtag.get("text"));
			}//end for
			result.append(hashtags.toString());
		} else {
			result.append("");
		}//end if
		result.append(DELIMITER);
		//process media
		if (!((List<Map<String,Object>>)((Map<String,Object>)map.get("entities")).get("media")).isEmpty()) {
			StringBuffer mediaUrls = new StringBuffer();
			//process
			for (Map<String,Object> hashtag: (List<Map<String,Object>>)((Map<String,Object>)map.get("entities")).get("media")) {
				if (mediaUrls.length() > 0) {
					mediaUrls.append(ARRAY_DELIMITER);
				}//end if
				mediaUrls.append(hashtag.get("media_url"));
			}//end for
			result.append(mediaUrls.toString());			
		} else {
			result.append("");
		}//end if
		//return
		return result.toString();
	}
	
	/**
	target format:
	id_str,created_at,in_reply_to_status_id_str,lang,source,text,user.created_at,user.id_str,user.name,user.screen_name,user.location,entities.hashtags[].text,entities.media.media_url
	
	source format:
{
	"contributors": null,
	"coordinates": null,
	"created_at": "Sun May 31 10:16:24 +0000 2015",
	"entities": 
	{
		"hashtags": 
		[
			{
				"indices": 
				[
					0,
					12
				],

				"text": "BahrainLive"
			},

			{
				"indices": 
				[
					13,
					21
				],

				"text": "Bahrain"
			},

			{
				"indices": 
				[
					22,
					28
				],

				"text": "sudia"
			},

			{
				"indices": 
				[
					29,
					36
				],

				"text": "kuwait"
			},

			{
				"indices": 
				[
					37,
					43
				],

				"text": "Qatar"
			},

			{
				"indices": 
				[
					44,
					54
				],

				"text": "Qatar2022"
			},

			{
				"indices": 
				[
					55,
					61
				],

				"text": "Dubai"
			},

			{
				"indices": 
				[
					62,
					67
				],

				"text": "oman"
			},

			{
				"indices": 
				[
					68,
					72
				],

				"text": "ksa"
			},

			{
				"indices": 
				[
					73,
					81
				],

				"text": "emarate"
			},

			{
				"indices": 
				[
					82,
					86
				],

				"text": "uae"
			},

			{
				"indices": 
				[
					87,
					92
				],

				"text": "Iraq"
			},

			{
				"indices": 
				[
					93,
					99
				],

				"text": "Egypt"
			},

			{
				"indices": 
				[
					100,
					107
				],

				"text": "jordan"
			},

			{
				"indices": 
				[
					108,
					113
				],

				"text": "yame"
			}
		],

		"media": 
		[
			{
				"display_url": "pic.twitter.com/ZzjPR2Zgbb",
				"expanded_url": "http://twitter.com/4God_sake/status/604954572717584384/photo/1",
				"id": 604954563813064704,
				"id_str": "604954563813064704",
				"indices": 
				[
					114,
					136
				],

				"media_url": "http://pbs.twimg.com/media/CGU6-HlUQAAyYYY.jpg",
				"media_url_https": "https://pbs.twimg.com/media/CGU6-HlUQAAyYYY.jpg",
				"sizes": 
				{
					"large": 
					{
						"h": 960,
						"resize": "fit",
						"w": 640
					},

					"medium": 
					{
						"h": 900,
						"resize": "fit",
						"w": 600
					},

					"small": 
					{
						"h": 510,
						"resize": "fit",
						"w": 340
					},

					"thumb": 
					{
						"h": 150,
						"resize": "crop",
						"w": 150
					}
				},

				"type": "photo",
				"url": "http://t.co/ZzjPR2Zgbb"
			}
		],

		"symbols": 
		[
			
		],

		"urls": 
		[
			
		],

		"user_mentions": 
		[
			
		]
	},

	"favorite_count": 0,
	"favorited": false,
	"geo": null,
	"id": 604954572717584384,
	"id_str": "604954572717584384",
	"in_reply_to_screen_name": null,
	"in_reply_to_status_id": null,
	"in_reply_to_status_id_str": null,
	"in_reply_to_user_id": null,
	"in_reply_to_user_id_str": null,
	"is_quote_status": false,
	"lang": "und",
	"metadata": 
	{
		"iso_language_code": "und",
		"result_type": "recent"
	},

	"place": null,
	"possibly_sensitive": false,
	"retweet_count": 0,
	"retweeted": false,
	"source": "<a href=\"http://twitter.com/download/android\" rel=\"nofollow\">Twitter for Android</a>",
	"text": "#BahrainLive #Bahrain #sudia #kuwait #Qatar #Qatar2022 #Dubai #oman #ksa #emarate #uae #Iraq #Egypt #jordan #yame http://t.co/ZzjPR2Zgbb",
	"truncated": false,
	"user": 
	{
		"contributors_enabled": false,
		"created_at": "Thu Jan 26 09:24:26 +0000 2012",
		"default_profile": true,
		"default_profile_image": false,
		"description": "",
		"entities": 
		{
			"description": 
			{
				"urls": 
				[
					
				]
			}
		},

		"favourites_count": 632,
		"follow_request_sent": null,
		"followers_count": 74,
		"following": null,
		"friends_count": 310,
		"geo_enabled": false,
		"id": 474746622,
		"id_str": "474746622",
		"is_translation_enabled": false,
		"is_translator": false,
		"lang": "en",
		"listed_count": 0,
		"location": "Bahrain",
		"name": "رحمتك يا رب العالمين",
		"notifications": null,
		"profile_background_color": "C0DEED",
		"profile_background_image_url": "http://abs.twimg.com/images/themes/theme1/bg.png",
		"profile_background_image_url_https": "https://abs.twimg.com/images/themes/theme1/bg.png",
		"profile_background_tile": false,
		"profile_image_url": "http://pbs.twimg.com/profile_images/467254967783276544/cAnLHT17_normal.jpeg",
		"profile_image_url_https": "https://pbs.twimg.com/profile_images/467254967783276544/cAnLHT17_normal.jpeg",
		"profile_link_color": "0084B4",
		"profile_sidebar_border_color": "C0DEED",
		"profile_sidebar_fill_color": "DDEEF6",
		"profile_text_color": "333333",
		"profile_use_background_image": true,
		"protected": false,
		"screen_name": "4God_sake",
		"statuses_count": 5466,
		"time_zone": "Kuwait",
		"url": null,
		"utc_offset": 10800,
		"verified": false
	}
}

	 */
}
