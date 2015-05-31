### twitter 'flattener' 

simple transformer to 'flatten' tweets into a '|' delimited line

takes in a full tweet from spring-xd and converts to the following

```
id_str|created_at|in_reply_to_status_id_str|lang|source|text|user.created_at|user.id_str|user.name|user.screen_name|user.location|entities.hashtags[].text|entities.media.media_url
```

### build and installation

```
mvn clean package
shell/bin/xd-shell
module upload --file [jar location] --type processor --name [choose a name]
```

### use in SpringXD
```
stream create --name rawtweets --definition "twittersearch --consumerKey='[your key]' --consumerSecret='[your secret]' --query='[query]' | file" --deploy

stream create --name formattedtweets --definition "tap:stream:rawtweets > [module-name] | file" --deploy
```
