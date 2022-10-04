import psycopg
from datetime import datetime
import datetime as dt
import pandas as pd
import csv
import time



def prepareAuthors(json_file):
    start = time.time()
    # Needs id otherwise returns bad int, pandas bug ?
    timefile = open('../TimeLogs/createImportFilesAuthorsTime.csv', 'w')
    timewriter = csv.writer(timefile, delimiter=';', quoting=csv.QUOTE_MINIMAL, lineterminator = '\n')
    chunks = pd.read_json(json_file, lines=True, chunksize=10000, dtype=False, encoding='utf-8')
    counter = 1
    for chunk in chunks:
        smallstart = time.time()
        print(str(counter))
        chunk = chunk.reset_index()
        chunk = chunk.drop_duplicates('id')
        chunk = chunk.reset_index()
        chunk = chunk.replace(r'\n', ' ', regex=True)
        chunk = chunk.replace(r'\r', '', regex=True)
        chunk = chunk.replace(r';', ',', regex=True)
        chunk = chunk.replace(r'\x00', '\uFFFD', regex=True)
        chunk = chunk.replace(r'\\', '/', regex=True)
        public_metrics = chunk['public_metrics'].values.tolist()
        rate = pd.DataFrame(public_metrics,
                            columns=['followers_count', 'following_count', 'tweet_count', 'listed_count'])
        chunk = pd.concat([chunk, rate], axis="columns")
        chunk = chunk[['id', 'name', 'username', 'description', 'followers_count', 'following_count', 'tweet_count',
                       'listed_count']]
        # print(chunk.to_string())
        # Works but not optimal
        # engine = create_engine('postgresql://postgres:neuhadnes3@localhost:5432/postgres')
        # chunk.to_sql('authors', engine, if_exists='append',index=False)
        chunk.to_csv('Authors/authors' + str(counter) + '.csv', header=False, index=False, sep=';', encoding='utf-8')
        timewriter.writerow(
            [str(datetime.now()).split(".")[0], str(dt.timedelta(seconds=time.time() - start)).split(".")[0],
             str(dt.timedelta(seconds=time.time() - smallstart)).split(".")[0]])
        counter += 1
    end = time.time()
    print(end - start)
    print("Author tables are prepared for insert")
    end = time.time()
    print(end - start)


def prepareConversations(json_file):
    start = time.time()
    chunks = pd.read_json(json_file, lines=True, chunksize=10000, dtype=False)
    #print("readlo to")
    counter = 1
    #cur.execute("TRUNCATE authors;")
    timefile = open('../TimeLogs/createImportFilesConversationsTime.csv', 'w')
    timewriter = csv.writer(timefile, delimiter=';', quoting=csv.QUOTE_MINIMAL, lineterminator = '\n')
    #usedIDs = []
    for chunk in chunks:
        smallstart = time.time()
        chunk = chunk.reset_index()
        chunk = chunk.drop_duplicates('id')
        chunk = chunk.reset_index()
        chunk = chunk.replace(r'\n', ' ', regex=True)
        chunk = chunk.replace(r'\r', '', regex=True)
        chunk = chunk.replace(r';', ',', regex=True)
        chunk = chunk.replace(r'\x00', '\uFFFD', regex=True)
        chunk = chunk.replace(r'\\', '/', regex=True)
        #print(chunk.to_string())
        #Prepare the conversations
        #chunk = chunk[~chunk['id'].isin(usedIDs)]
        #usedIDs = usedIDs + chunk['id'].tolist()
        public_metrics = chunk['public_metrics'].values.tolist()
        counts = pd.DataFrame(public_metrics,
                            columns=['retweet_count', 'reply_count', 'like_count', 'quote_count'])
        conversations = pd.concat([chunk, counts], axis="columns")
        conversations = conversations[['id', 'author_id', 'text', 'possibly_sensitive', 'lang', 'source', 'created_at']]
        conversations = pd.concat([conversations, counts], axis="columns")
        conversations = conversations[['id', 'author_id', 'text', 'possibly_sensitive', 'lang', 'source', 'retweet_count', 'reply_count', 'like_count', 'quote_count', 'created_at']]
        #print(len(usedIDs))
        #print(conversations['id'].isin(usedIDs))
        conversations.to_csv('conversations/conversations' + str(counter) + '.csv', header=False, index=False, sep=';', encoding='utf-8')
        #print(conversations.to_string())
        #Prepare the entities: hashtags, urls and annotations
        entities = chunk['entities'].values.tolist()
        entities = pd.json_normalize(entities)
        #print(entities.to_string())
        entities = entities[['hashtags', 'urls', 'annotations']]
        entities = pd.concat([entities, conversations['id']], axis="columns")
        entities = pd.concat([entities, chunk['referenced_tweets']], axis="columns")
        entities = pd.concat([entities, chunk['context_annotations']], axis="columns")
        #print(entities.to_string())

        #Prepare the URLS
        urls = entities[['id','urls']]
        #print(urls.to_string())
        tmp = urls.explode('urls').reset_index(drop=True)
        #print(tmp.to_string())
        urls = pd.concat([tmp, pd.json_normalize(tmp['urls'])], axis=1).drop('urls', axis=1)
        urls = urls[['id','expanded_url', 'title', 'description']]
        urls = urls[urls['expanded_url'].notna()]
        urls.to_csv('Urls/urls' + str(counter) + '.csv', header=False, index=False, sep=';', encoding='utf-8')
        #print(urls.to_string())

        #Prepare the hashtags
        hashtags = entities[['id', 'hashtags']]
        # print(urls.to_string())
        tmp = hashtags.explode('hashtags').reset_index(drop=True)
        # print(tmp.to_string())
        hashtags = pd.concat([tmp, pd.json_normalize(tmp['hashtags'])], axis=1).drop('hashtags', axis=1)
        hashtags = hashtags[['id', 'tag']]
        hashtags = hashtags[hashtags['tag'].notna()]
        hashtags.to_csv('Conversations_hashtags/hashtags' + str(counter) + '.csv', header=False, index=False, sep=';', encoding='utf-8')
        hashtags['tag'].to_csv('Hashtags/just_hashtags' + str(counter) + '.csv', header=True, index=False, sep=';', encoding='utf-8')
        #print(hashtags.to_string())

        #Prepare the annotations
        annotations = entities[['id', 'annotations']]
        # print(urls.to_string())
        tmp = annotations.explode('annotations').reset_index(drop=True)
        # print(tmp.to_string())
        annotations = pd.concat([tmp, pd.json_normalize(tmp['annotations'])], axis=1).drop('annotations', axis=1)
        annotations = annotations[['id', 'normalized_text', 'type', 'probability']]
        annotations = annotations[annotations['normalized_text'].notna()]
        annotations.to_csv('Annotations/annotations' + str(counter) + '.csv', header=False, index=False, sep=';', encoding='utf-8')
        #print(annotations.to_string())

        #Prepare the conversation_references
        referenced_tweets = entities[['id', 'referenced_tweets']]
        referenced_tweets = referenced_tweets.rename(columns={'id': "child_id"})
        #print(referenced_tweets.to_string())
        tmp = referenced_tweets.explode('referenced_tweets').reset_index(drop=True)
        # print(tmp.to_string())
        referenced_tweets = pd.concat([tmp, pd.json_normalize(tmp['referenced_tweets'])], axis=1).drop('referenced_tweets', axis=1)
        referenced_tweets = referenced_tweets[['child_id', 'id', 'type']]
        referenced_tweets = referenced_tweets[referenced_tweets['type'].notna()]
        referenced_tweets.to_csv('referenced_tweets/referenced_tweets' + str(counter) + '.csv', header=False, index=False, sep=';', encoding='utf-8')
        #print(referenced_tweets.to_string())

        #Prepare the context_annotations
        context_annotations = chunk[['id','context_annotations']]
        tmp = context_annotations.explode('context_annotations').reset_index(drop=True)
        # print(tmp.to_string())
        context_annotations = pd.concat([tmp, pd.json_normalize(tmp['context_annotations'])], axis=1).drop(
            'context_annotations', axis=1)
        #print(context_annotations)
        #context_annotations = context_annotations[['child_id', 'id', 'type']]
        #context_annotations = context_annotations[context_annotations['type'].notna()]
        context_annotations[context_annotations['domain.id'].notna()][['id','domain.id', 'entity.id']].to_csv('context_annotations/context_annotations' + str(counter) + '.csv', header=False, index=False, sep=';', encoding='utf-8')
        context_annotations[context_annotations['domain.id'].notna()][['domain.id', 'domain.name',  'domain.description']].to_csv('context_domains/context_domains' + str(counter) + '.csv', header=False, index=False, sep=';', encoding='utf-8')
        context_annotations[context_annotations['entity.id'].notna()][['entity.id', 'entity.name',  'entity.description']].to_csv('context_entities/context_entities' + str(counter) + '.csv', header=False, index=False, sep=';', encoding='utf-8')
        #print(context_annotations.to_string())
        #entities = pd.DataFrame(entities,
                            #columns=['annotations', 'cashtags', 'hashtags', 'mentions', 'urls'])
        #chunk = chunk[['hashtags']]
        #print(entities.to_string())
        #print(counter)
        #buffer = io.StringIO()
        #conversations.to_csv(buffer, header=False, index=False, sep=';', encoding='utf-8')
        #conversations.to_csv('./Conversations/conversations'+ str(counter) +'.csv', header=False, index=False, sep=';', encoding='utf-8')
        #conversations.to_csv('./Urls/urls' + str(counter) + '.csv', header=False, index=False, sep=';', encoding='utf-8')
        #buffer.seek(0)
        #print(buffer)
        #cur.copy_from(buffer, 'conversations', sep=";")
        #cur.copy_from(open('just_hashtags.csv', 'r', encoding='utf-8'), 'hashtags_temp', sep=";", columns=["tag"])
        #cur.execute(copy_hashtags)
        """
        with open("conversations.csv", "r", encoding='utf-8') as f:
            with cur.copy("COPY conversations_temp FROM STDIN (DELIMITER ';',FORMAT 'csv')") as copy:
                while data := f.read(10000):
                    copy.write(data)
        with open("just_hashtags.csv", "r", encoding='utf-8') as f:
            with cur.copy("COPY hashtags_temp (tag) FROM STDIN (DELIMITER ';',FORMAT 'csv')") as copy:
                while data := f.read(10000):
                    copy.write(data)
        """
        print(counter)
        counter += 1
        timewriter.writerow([str(datetime.now()).split(".")[0], str(dt.timedelta(seconds=time.time() - start)).split(".")[0], str(dt.timedelta(seconds=time.time() - smallstart)).split(".")[0]])
        #print(str(datetime.now()) +";" + str(dt.timedelta(seconds=time.time() - start)).split(".")[0] + ";" + str(dt.timedelta(seconds=time.time() - smallstart)).split(".")[0])
        smallend = time.time()
        print(smallend - smallstart)
    #cur.execute(copy_conversations)
    end = time.time()
    print(end - start)


def insertAuthors(timewriter, start):
    #start = time.time()
    # Needs id otherwise returns bad int, pandas bug ?
    conn = psycopg.connect("dbname=postgres user=postgres password=neuhadnes3")
    cur = conn.cursor()
    command = """
        DROP TABLE IF EXISTS authors;

        CREATE TABLE IF NOT EXISTS authors (
            id bigint,
            name VARCHAR(255),
            username VARCHAR(255),
            description text,
            followers_count integer,
            following_count integer,
            tweet_count integer,
            listed_count integer
        );
        """
    authors_add_pk = """
    DELETE FROM authors USING authors at2
          WHERE authors.id = at2.id AND authors.ctid < at2.ctid;
    ALTER TABLE authors
      ADD PRIMARY KEY (id);
    """
    cur.execute(command)
    counter = 1
    try:
        while(1):
            smallstart = time.time()
            print(str(counter))
            with open("Authors/authors" + str(counter) + ".csv", "r", encoding='utf-8') as f:
                with cur.copy("COPY authors FROM STDIN (DELIMITER ';')") as copy:
                    while data := f.read(10000):
                        copy.write(data)
            counter += 1
            timewriter.writerow(
                [str(datetime.now()).split(".")[0], str(dt.timedelta(seconds=time.time() - start)).split(".")[0],
                 str(dt.timedelta(seconds=time.time() - smallstart)).split(".")[0]])
    except FileNotFoundError :
        print("All author files inserted")
        print(str(time.time() - start))
    conn.commit()
    print("Copy from files complete, deleting duplicates and adding constraints")
    smallstart = time.time()
    cur.execute(authors_add_pk)
    timewriter.writerow(
        [str(datetime.now()).split(".")[0], str(dt.timedelta(seconds=time.time() - start)).split(".")[0],
         str(dt.timedelta(seconds=time.time() - smallstart)).split(".")[0]])
    # close communication with the PostgreSQL database server
    cur.close()
    # commit the changes
    conn.commit()
    print("Authors inserted")


def insertConversations(timewriter, start):
    #start = time.time()
    # print("readlo to")
    conn = psycopg.connect("dbname=postgres user=postgres password=neuhadnes3")
    cur = conn.cursor()
    # cur.execute("TRUNCATE authors;")
    command = """
        DROP TABLE IF EXISTS conversations;

        CREATE TABLE IF NOT EXISTS conversations (
            id bigint,
            author_id BIGINT NOT NULL,
            content text NOT NULL,
            possibly_sensitive bool NOT NULL,
            language varchar(3) NOT NULL,
            source text NOT NULL,
            retweet_count integer,
            reply_count integer,
            like_count integer,
            quote_count integer,
            created_at timestamp with time zone NOT NULL
        );
        """
    conversations_remove_duplicate_id = """
    DELETE FROM conversations USING conversations at2
	    WHERE conversations.id = at2.id AND conversations.ctid < at2.ctid;
	
    ALTER TABLE conversations
      ADD PRIMARY KEY (id);

        """

    conversations_remove_invalid_foreign_keys = """
    DELETE FROM conversations lg
        WHERE  NOT EXISTS (
           SELECT FROM authors lr
           WHERE  lr.id = lg.author_id
           );     
	     
    ALTER TABLE conversations
        ADD FOREIGN KEY (author_id) REFERENCES authors(id); 
        """

    conversations_add_pk_if_missing = """
    INSERT INTO authors (id)
    SELECT DISTINCT author_id FROM conversations lg
            WHERE  NOT EXISTS (
               SELECT FROM authors lr
               WHERE  lr.id = lg.author_id
               );   
		   
   ALTER TABLE conversations
        ADD FOREIGN KEY (author_id) REFERENCES authors(id);
    """
    cur.execute(command)
    counter = 1
    try:
        while(1):
            smallstart = time.time()
            print(str(counter))
            with open("Conversations/conversations" + str(counter) + ".csv", "r", encoding='utf-8') as f:
                with cur.copy("COPY conversations FROM STDIN (DELIMITER ';',FORMAT 'csv')") as copy:
                    while data := f.read(10000):
                        copy.write(data)
            timewriter.writerow(
                [str(datetime.now()).split(".")[0], str(dt.timedelta(seconds=time.time() - start)).split(".")[0],
                 str(dt.timedelta(seconds=time.time() - smallstart)).split(".")[0]])
            counter += 1
    except FileNotFoundError :
        print("All conversation files inserted")
        print(str(time.time() - start))
    conn.commit()
    duplicatestart = time.time()
    smallstart = time.time()
    cur.execute(conversations_remove_duplicate_id)
    timewriter.writerow(
        [str(datetime.now()).split(".")[0], str(dt.timedelta(seconds=time.time() - start)).split(".")[0],
         str(dt.timedelta(seconds=time.time() - smallstart)).split(".")[0]])
    conn.commit()
    print(str(time.time()- duplicatestart))
    duplicatestart = time.time()
    smallstart = time.time()
    #cur.execute(conversations_remove_invalid_foreign_keys)
    cur.execute(conversations_add_pk_if_missing)
    conn.commit()
    timewriter.writerow(
        [str(datetime.now()).split(".")[0], str(dt.timedelta(seconds=time.time() - start)).split(".")[0],
         str(dt.timedelta(seconds=time.time() - smallstart)).split(".")[0]])
    cur.close()


def insertHashtags(timewriter, start):
    #start = time.time()
    conn = psycopg.connect("dbname=postgres user=postgres password=neuhadnes3")
    cur = conn.cursor()
    command = """
            DROP TABLE IF EXISTS hashtags;

            CREATE TABLE IF NOT EXISTS hashtags (
                id BIGSERIAL PRIMARY KEY,
                tag text UNIQUE NOT NULL);
                
            DROP TABLE IF EXISTS hashtags_temp;

            CREATE TABLE IF NOT EXISTS hashtags_temp (
                id BIGSERIAL PRIMARY KEY,
                tag text
            );
            """
    hashtags_remove_duplicates = """
       INSERT INTO hashtags
        SELECT * FROM hashtags_temp
        ON CONFLICT DO NOTHING;
        
        DROP TABLE IF EXISTS hashtags_temp;

            """
    cur.execute(command)
    counter = 1
    try:
        while (1):
            smallstart = time.time()
            print(str(counter))
            with open("Hashtags/just_hashtags" + str(counter) + ".csv", "r", encoding='utf-8') as f:
                with cur.copy("COPY hashtags_temp (tag) FROM STDIN (DELIMITER ';',FORMAT 'csv')") as copy:
                    while data := f.read(10000):
                        copy.write(data)
            counter += 1
            timewriter.writerow(
                [str(datetime.now()).split(".")[0],
                 str(dt.timedelta(seconds=time.time() - start)).split(".")[0],
                 str(dt.timedelta(seconds=time.time() - smallstart)).split(".")[0]])
    except FileNotFoundError:
        print("All hashtags files inserted")
        print(str(time.time() - start))
    conn.commit()
    smallstart = time.time()
    cur.execute(hashtags_remove_duplicates)
    timewriter.writerow(
        [str(datetime.now()).split(".")[0],
         str(dt.timedelta(seconds=time.time() - start)).split(".")[0],
         str(dt.timedelta(seconds=time.time() - smallstart)).split(".")[0]])
    conn.commit()
    cur.close()

def insertLinks(timewriter, start):
    #start = time.time()
    conn = psycopg.connect("dbname=postgres user=postgres password=neuhadnes3")
    cur = conn.cursor()
    command = """
            DROP TABLE IF EXISTS links;

            CREATE TABLE IF NOT EXISTS links (
                id BIGSERIAL PRIMARY KEY,
                conversation_id BIGINT NOT NULL,
                url varchar(4086) NOT NULL,
                title text,
                description text
                );
            """

    links_remove_bad_fk = """ 
    ALTER TABLE links
        ADD FOREIGN KEY (conversation_id) REFERENCES conversations(id); 
        """

    links_remove_long_urls = """
    
    DELETE FROM links where length(url) > 2048;

    ALTER TABLE links ALTER COLUMN url TYPE varchar(2048);
        """
    cur.execute(command)
    counter = 1
    try:
        while (1):
            smallstart = time.time()
            print(str(counter))
            with open("Urls/urls" + str(counter) + ".csv", "r", encoding='utf-8') as f:
                with cur.copy("COPY links (conversation_id, url, title, description) FROM STDIN (DELIMITER ';',FORMAT 'csv')") as copy:
                    while data := f.read(10000):
                        copy.write(data)
            counter += 1
            timewriter.writerow(
                [str(datetime.now()).split(".")[0], str(dt.timedelta(seconds=time.time() - start)).split(".")[0],
                 str(dt.timedelta(seconds=time.time() - smallstart)).split(".")[0]])
    except FileNotFoundError:
        print("All link files inserted")
        print(str(time.time() - start))
    conn.commit()
    smallstart = time.time()
    cur.execute(links_remove_bad_fk)
    timewriter.writerow(
        [str(datetime.now()).split(".")[0], str(dt.timedelta(seconds=time.time() - start)).split(".")[0],
         str(dt.timedelta(seconds=time.time() - smallstart)).split(".")[0]])
    conn.commit()
    smallstart = time.time()
    cur.execute(links_remove_long_urls)
    timewriter.writerow(
        [str(datetime.now()).split(".")[0], str(dt.timedelta(seconds=time.time() - start)).split(".")[0],
         str(dt.timedelta(seconds=time.time() - smallstart)).split(".")[0]])
    conn.commit()
    cur.close()

def insertAnnotations(timewriter, start):
    #start = time.time()
    conn = psycopg.connect("dbname=postgres user=postgres password=neuhadnes3")
    cur = conn.cursor()
    command = """
            DROP TABLE IF EXISTS annotations;

            CREATE TABLE IF NOT EXISTS annotations (
                id BIGSERIAL PRIMARY KEY,
                conversation_id BIGINT NOT NULL,
                value text ,
                type text NOT NULL,
                probability numeric(4,3) NOT NULL
                );
            """

    annotations_remove_bad_fk = """
    DELETE FROM annotations lg
        WHERE  NOT EXISTS (
           SELECT FROM conversations lr
           WHERE  lr.id = lg.conversation_id
           );     

    ALTER TABLE annotations
        ADD FOREIGN KEY (conversation_id) REFERENCES conversations(id); 
        """

    cur.execute(command)
    counter = 1
    try:
        while (1):
            smallstart = time.time()
            print(str(counter))
            with open("Annotations/annotations" + str(counter) + ".csv", "r", encoding='utf-8') as f:
                with cur.copy("COPY annotations (conversation_id, value, type, probability) FROM STDIN (DELIMITER ';',FORMAT 'csv')") as copy:
                    while data := f.read(10000):
                        copy.write(data)
            counter += 1
            timewriter.writerow(
                [str(datetime.now()).split(".")[0], str(dt.timedelta(seconds=time.time() - start)).split(".")[0],
                 str(dt.timedelta(seconds=time.time() - smallstart)).split(".")[0]])
    except FileNotFoundError:
        print("All annotations files inserted")
        print(str(time.time() - start))
    conn.commit()
    smallstart = time.time()
    cur.execute(annotations_remove_bad_fk)
    timewriter.writerow(
        [str(datetime.now()).split(".")[0], str(dt.timedelta(seconds=time.time() - start)).split(".")[0],
         str(dt.timedelta(seconds=time.time() - smallstart)).split(".")[0]])
    conn.commit()
    cur.close()


def insertConversationReferences(timewriter, start):
    #start = time.time()
    conn = psycopg.connect("dbname=postgres user=postgres password=neuhadnes3")
    cur = conn.cursor()
    command = """
            DROP TABLE IF EXISTS conversation_references;

            CREATE TABLE IF NOT EXISTS conversation_references (
                id BIGSERIAL PRIMARY KEY,
                conversation_id BIGINT NOT NULL,
                parent_id BIGINT NOT NULL,
                type varchar(20) NOT NULL
                );
            """

    conversation_references_remove_bad_fk = """

    ALTER TABLE conversation_references
        ADD FOREIGN KEY (conversation_id) REFERENCES conversations(id); 
        
    DELETE FROM conversation_references lg
    WHERE  NOT EXISTS (
       SELECT FROM conversations lr
       WHERE  lr.id = lg.parent_id
       );     

    ALTER TABLE conversation_references
        ADD FOREIGN KEY (parent_id) REFERENCES conversations(id); 
        """

    cur.execute(command)
    counter = 1
    try:
        while (1):
            smallstart = time.time()
            print(str(counter))
            with open("Referenced_tweets/referenced_tweets" + str(counter) + ".csv", "r", encoding='utf-8') as f:
                with cur.copy("COPY conversation_references (conversation_id, parent_id, type) FROM STDIN (DELIMITER ';',FORMAT 'csv')") as copy:
                    while data := f.read(10000):
                        copy.write(data)
            counter += 1
            timewriter.writerow(
                [str(datetime.now()).split(".")[0], str(dt.timedelta(seconds=time.time() - start)).split(".")[0],
                 str(dt.timedelta(seconds=time.time() - smallstart)).split(".")[0]])
    except FileNotFoundError:
        print("All conversation reference files inserted")
        print(str(time.time() - start))
    conn.commit()
    smallstart = time.time()
    cur.execute(conversation_references_remove_bad_fk)
    timewriter.writerow(
        [str(datetime.now()).split(".")[0], str(dt.timedelta(seconds=time.time() - start)).split(".")[0],
         str(dt.timedelta(seconds=time.time() - smallstart)).split(".")[0]])
    conn.commit()
    cur.close()


def insertContextDomains(timewriter, start):
    #start = time.time()
    conn = psycopg.connect("dbname=postgres user=postgres password=neuhadnes3")
    cur = conn.cursor()
    command = """
            DROP TABLE IF EXISTS context_domains;

            CREATE TABLE IF NOT EXISTS context_domains (
                id BIGSERIAL PRIMARY KEY,
                name varchar(255) NOT NULL,
                description text
                );
                
            DROP TABLE IF EXISTS context_domains_temp;

            CREATE TABLE IF NOT EXISTS context_domains_temp (
                id BIGINT,
                name varchar(255),
                description text
                );
            """
    domains_remove_duplicates = """
        INSERT INTO context_domains
        SELECT * FROM context_domains_temp
        ON CONFLICT DO NOTHING;
        
        DROP TABLE IF EXISTS context_domains_temp
            """
    cur.execute(command)
    counter = 1
    try:
        while (1):
            print(str(counter))
            smallstart = time.time()
            with open("Context_domains/Context_domains" + str(counter) + ".csv", "r", encoding='utf-8') as f:
                with cur.copy("COPY context_domains_temp FROM STDIN (DELIMITER ';',FORMAT 'csv')") as copy:
                    while data := f.read(10000):
                        copy.write(data)
                        timewriter.writerow(
                            [str(datetime.now()).split(".")[0],
                             str(dt.timedelta(seconds=time.time() - start)).split(".")[0],
                             str(dt.timedelta(seconds=time.time() - smallstart)).split(".")[0]])

            counter += 1
    except FileNotFoundError:
        print("All context domains files inserted")
        print(str(time.time() - start))
    conn.commit()
    smallstart = time.time()
    cur.execute(domains_remove_duplicates)
    timewriter.writerow(
        [str(datetime.now()).split(".")[0], str(dt.timedelta(seconds=time.time() - start)).split(".")[0],
         str(dt.timedelta(seconds=time.time() - smallstart)).split(".")[0]])
    conn.commit()
    cur.close()


def insertContextEntities(timewriter, start):
    #start = time.time()
    conn = psycopg.connect("dbname=postgres user=postgres password=neuhadnes3")
    cur = conn.cursor()
    command = """
            DROP TABLE IF EXISTS context_entities;

            CREATE TABLE IF NOT EXISTS context_entities (
                id BIGSERIAL PRIMARY KEY,
                name varchar(255) NOT NULL,
                description text
                );

            DROP TABLE IF EXISTS context_entities_temp;

            CREATE TABLE IF NOT EXISTS context_entities_temp (
                id BIGINT,
                name varchar(255),
                description text
                );
            """
    domains_remove_duplicates = """
        INSERT INTO context_entities
        SELECT * FROM context_entities_temp
        ON CONFLICT DO NOTHING;

        DROP TABLE IF EXISTS context_entities_temp
            """
    cur.execute(command)
    counter = 1
    try:
        while (1):
            print(str(counter))
            smallstart = time.time()
            with open("context_entities/context_entities" + str(counter) + ".csv", "r", encoding='utf-8') as f:
                with cur.copy("COPY context_entities_temp FROM STDIN (DELIMITER ';',FORMAT 'csv')") as copy:
                    while data := f.read(10000):
                        copy.write(data)
                        timewriter.writerow(
                            [str(datetime.now()).split(".")[0],
                             str(dt.timedelta(seconds=time.time() - start)).split(".")[0],
                             str(dt.timedelta(seconds=time.time() - smallstart)).split(".")[0]])

            counter += 1
    except FileNotFoundError:
        print("All context entities files inserted")
        print(str(time.time() - start))
    conn.commit()
    smallstart = time.time()
    cur.execute(domains_remove_duplicates)
    timewriter.writerow(
        [str(datetime.now()).split(".")[0], str(dt.timedelta(seconds=time.time() - start)).split(".")[0],
         str(dt.timedelta(seconds=time.time() - smallstart)).split(".")[0]])
    conn.commit()
    cur.close()

def insertContextAnnotations(timewriter, start):
    #start = time.time()
    conn = psycopg.connect("dbname=postgres user=postgres password=neuhadnes3")
    cur = conn.cursor()
    command = """
            DROP TABLE IF EXISTS context_annotations;

            CREATE TABLE IF NOT EXISTS context_annotations (
                id BIGSERIAL PRIMARY KEY,
                conversation_id BIGINT,
                context_domain_id BIGINT,
                context_entity_id BIGINT               
                );

            """
    annotations_remove_bad_fk = """
            DELETE FROM context_annotations lg
                WHERE  NOT EXISTS (
                   SELECT FROM conversations lr
                   WHERE  lr.id = lg.conversation_id
                   );     

    ALTER TABLE context_annotations
        ADD FOREIGN KEY (conversation_id) REFERENCES conversations(id); 
        
    ALTER TABLE context_annotations
        ADD FOREIGN KEY (context_entity_id) REFERENCES context_entities(id); 
    
    ALTER TABLE context_annotations
        ADD FOREIGN KEY (context_domain_id) REFERENCES context_domains(id); 
        
            """
    cur.execute(command)
    counter = 1
    try:
        while (1):
            smallstart = time.time()
            print(str(counter))
            with open("Context_annotations/Context_annotations" + str(counter) + ".csv", "r", encoding='utf-8') as f:
                with cur.copy("COPY Context_annotations (conversation_id, context_domain_id, context_entity_id) FROM STDIN (DELIMITER ';',FORMAT 'csv')") as copy:
                    while data := f.read(10000):
                        copy.write(data)
            counter += 1
            timewriter.writerow(
                [str(datetime.now()).split(".")[0],
                 str(dt.timedelta(seconds=time.time() - start)).split(".")[0],
                 str(dt.timedelta(seconds=time.time() - smallstart)).split(".")[0]])
    except FileNotFoundError:
        print("All context annotations files inserted")
        print(str(time.time() - start))
    conn.commit()
    smallstart = time.time()
    cur.execute(annotations_remove_bad_fk)
    timewriter.writerow(
        [str(datetime.now()).split(".")[0],
         str(dt.timedelta(seconds=time.time() - start)).split(".")[0],
         str(dt.timedelta(seconds=time.time() - smallstart)).split(".")[0]])
    conn.commit()
    cur.close()


def insertConversationsHashtags(timewriter, start):
    #start = time.time()
    conn = psycopg.connect("dbname=postgres user=postgres password=neuhadnes3")
    cur = conn.cursor()
    command = """
            DROP TABLE IF EXISTS conversation_hashtags;

            CREATE TABLE IF NOT EXISTS conversation_hashtags (
                id BIGSERIAL PRIMARY KEY,
                conversation_id BIGINT NOT NULL,
                tag text,  
                hashtag_id BIGINT      
                );

            """

    CH_add_fk = """
        UPDATE conversation_hashtags
        SET hashtag_id = hashtags.id
        FROM hashtags
        WHERE hashtags.tag = conversation_hashtags.tag;
        
        ALTER TABLE conversation_hashtags
            ADD FOREIGN KEY (hashtag_id) REFERENCES hashtags(id);
        
        ALTER TABLE conversation_hashtags
            DROP COLUMN tag;
            
        ALTER TABLE conversation_hashtags
            ADD FOREIGN KEY (conversation_id) REFERENCES conversations(id);
            
        ALTER TABLE conversation_hashtags ALTER COLUMN hashtag_id SET NOT NULL;
            """
    cur.execute(command)
    counter = 1
    try:
        while (1):
            smallstart = time.time()
            print(str(counter))
            with open("Conversations_hashtags/hashtags" + str(counter) + ".csv", "r", encoding='utf-8') as f:
                with cur.copy(
                        "COPY Conversation_hashtags (conversation_id, tag) FROM STDIN (DELIMITER ';',FORMAT 'csv')") as copy:
                    while data := f.read(10000):
                        copy.write(data)
            counter += 1
            timewriter.writerow(
                [str(datetime.now()).split(".")[0],
                 str(dt.timedelta(seconds=time.time() - start)).split(".")[0],
                 str(dt.timedelta(seconds=time.time() - smallstart)).split(".")[0]])
    except FileNotFoundError:
        print("All conversation_hashtag files inserted")
        print(str(time.time() - start))
    conn.commit()
    smallstart = time.time()
    cur.execute(CH_add_fk)
    timewriter.writerow(
        [str(datetime.now()).split(".")[0],
         str(dt.timedelta(seconds=time.time() - start)).split(".")[0],
         str(dt.timedelta(seconds=time.time() - smallstart)).split(".")[0]])
    conn.commit()
    cur.close()

#TODO Problem when something does not have entities, for example {0:{blabla}, 1: nan}
#TODO conversation_references pridat constraint az na konci, kedze moze referencovat este nepridane conversations

#Inserting authors: 300 sec give or take
#Copy into temp table and then into propar one: 40 sec give or take
#Copy into table without constraints, enforce constraints later: remove duplicates: 10 sec, add PK : 5 sec, seems much better


# Press the green button in the gutter to run the script.
if __name__ == '__main__':

    #prepareAuthors("./Twitter Data/authors.jsonl")
    #prepareConversations("./Twitter Data/conversations.jsonl")
    start = time.time()
    timefile = open('./TimeLogs/insertTables.csv', 'w')
    timewriter = csv.writer(timefile, delimiter=';', quoting=csv.QUOTE_MINIMAL, lineterminator='\n')
    insertAuthors(timewriter, start)
    insertConversations(timewriter, start)
    insertHashtags(timewriter, start)
    insertLinks(timewriter, start)
    insertAnnotations(timewriter, start)
    insertConversationReferences(timewriter, start)
    insertContextDomains(timewriter, start)
    insertContextEntities(timewriter, start)
    insertContextAnnotations(timewriter, start)
    insertConversationsHashtags(timewriter, start)


# 370 while always checking constraints afte 10k