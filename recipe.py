#import requests
import time
import random
import requests_cache

from bs4 import BeautifulSoup
import json
import uuid
from pydantic import BaseModel
from typing import Optional, List
from neo4j import GraphDatabase

# Neo4j Connection details
uri = "bolt://localhost:7687"
username = "neo4j"
password = "password"
MIN_DELAY = 20
MAX_DELAY = 60*2

DRIVER = GraphDatabase.driver(uri, auth=(username, password))


class Image(BaseModel):
    UUID: str
    url: Optional[str] = None
    height: Optional[int] = 0
    width: Optional[int] = 0
    description: Optional[str] = None


class Author(BaseModel):
    UUID: str
    name: Optional[str] = None
    url: Optional[str] = None


class Publisher(BaseModel):
    UUID: str
    name: Optional[str] = None
    url: Optional[str] = None
    logo: Optional[str] = None


class Nutrition(BaseModel):
    UUID: str
    servingSize: Optional[str] = None
    calories: Optional[str] = None
    fatContent: Optional[str] = None
    saturatedFatContent: Optional[str] = None
    carbohydrateContent: Optional[str] = None
    fiberContent: Optional[str] = None
    sugarContent: Optional[str] = None
    proteinContent: Optional[str] = None
    cholesterolContent: Optional[str] = None
    sodiumContent: Optional[str] = None


class RecipeInstructions(BaseModel):
    UUID: str
    name: Optional[str] = None
    text: Optional[str] = None
    url: Optional[str] = None
    image: Optional[str] = None  # Image UUID or URL


class Review(BaseModel):
    UUID: str
    author: str  # Author UUID
    ratingValue: Optional[str] = None
    worstRating: Optional[str] = None
    bestRating: Optional[str] = None
    reviewBody: Optional[str] = None
    datePublished: Optional[str] = None


class Video(BaseModel):
    UUID: str
    name: Optional[str] = None
    description: Optional[str] = None
    duration: Optional[str] = None
    thumbnailUrl: Optional[str] = None
    contentUrl: Optional[str] = None
    uploadDate: Optional[str] = None


class Recipe(BaseModel):
    UUID: str = str(uuid.uuid4())
    name: Optional[str] = None
    url: Optional[str] = None
    headline: Optional[str] = None
    author: Optional[str] = None  # Author UUID
    image: Optional[List[str]] = []  # List of images (URLs or UUIDs)
    datePublished: Optional[str] = None
    dateModified: Optional[str] = None
    publisher: Optional[str] = None  # Publisher UUID
    keywords: Optional[List[str]] = []
    cookTime: Optional[str] = None
    totalTime: Optional[str] = None
    description: Optional[str] = None
    recipeIngredient: Optional[List[str]] = []
    recipeInstruction: Optional[List[str]] = []
    nutrition: Optional[str] = None  # Nutrition UUID
    ratingValue: Optional[int] = None
    reviewCount: Optional[int] = None
    review: Optional[List[str]] = []
    recipeYield: Optional[str] = None
    video: Optional[str] = None


def insert_node(tx, node, label):
    tx.run(
        f"MERGE (n:{label} {{UUID: $uuid}})"
        f"SET n += $props",
        uuid=node.UUID,
        props=node.__dict__
    )


def insert_relationship(tx, source_uuid, target_uuid, relationship_type):
    tx.run(
        "MATCH (source), (target) "
        "WHERE source.UUID = $source_uuid AND target.UUID = $target_uuid "
        "MERGE (source)-[r:" + relationship_type + "]->(target)",
        source_uuid=source_uuid,
        target_uuid=target_uuid
    )


def check_url_exists(url):
    with DRIVER.session() as session:
        query = f"""MATCH (r:Recipe) WHERE r.url = "{url}" RETURN r"""
        res = session.run(query)
        if res.single() is not None:
            # Already crawled
            return True
        else:
            return False


def check_node_exists(UUID):
    with DRIVER.session() as session:
        # Query to check if an author with the same name exists
        query = """MATCH (n) WHERE n.UUID = "{UUID}" RETURN n"""
        res = session.run(query)
        if res.single() is not None:
            # Node exists
            return True
        else:
            return False


def extract_data(url):
    db_to_add = []
    session = requests_cache.CachedSession("crawl_cache")
    response = session.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')
    r = soup.find("script", {"type": "application/ld+json"})
    if r:
        r_data = json.loads(r.contents[0])[0]
        r_keys = r_data.keys()
        recipe = Recipe()
        recipe.UUID = str(uuid.uuid4())

        if "name" in r_keys:
            recipe.name = r_data["name"]
        if "url" in r_keys:
            recipe.url = r_data["url"]
        if "headline" in r_keys:
            recipe.headline = r_data["headline"]
        if "dateModified" in r_keys:
            recipe.dateModified = r_data["dateModified"]
        if "datePublished" in r_keys:
            recipe.datePublished = r_data["datePublished"]
        if "keywords" in r_keys:
            recipe.keywords = r_data["keywords"].split(",")
        if "cookTime" in r_keys:
            recipe.cookTime = r_data["cookTime"]
        if "totalTime" in r_keys:
            recipe.totalTime = r_data["totalTime"]
        if "description" in r_keys:
            recipe.description = r_data["description"]
        if "recipeIngredient" in r_keys:
            recipe.recipeIngredient = r_data["recipeIngredient"]

        if "author" in r_keys:
            try:
                a = r_data["author"][0]
                a["UUID"] = "AUTHOR_" + r_data["author"][0]["name"]
            except:
                a = r_data["author"]
                a["UUID"] = "AUTHOR_" + r_data["author"]["name"]
            author = Author(**a)
            recipe.author = author.UUID
            db_to_add.append({"tag": "Author", "node": author})
        if "image" in r_keys:
            for each in r_data["image"]:
                if type(each) == str:
                    recipe.image.append(each)
                else:
                    each["UUID"] = str(uuid.uuid4())
                    image = Image(**each)
                    recipe.image.append(image.UUID)
                    db_to_add.append({"tag": "Image", "node": image})
        if "publisher" in r_keys:
            a = {
                "UUID": str(uuid.uuid4()),
                "name": r_data["publisher"]["name"],
                "url": r_data["publisher"]["url"],
                "logo": r_data["publisher"]["logo"]["url"]  # don't bother
            }
            pub = Publisher(**a)
            recipe.publisher = pub.UUID
            db_to_add.append({"tag": "Publisher", "node": pub})
        if "nutrition" in r_keys:
            a = r_data["nutrition"]
            a["UUID"] = str(uuid.uuid4())
            nutrition = Nutrition(**a)
            recipe.nutrition = nutrition.UUID
            db_to_add.append({"tag": "Nutrition", "node": nutrition})
        if "recipeInstructions" in r_keys:
            for each in r_data["recipeInstructions"]:
                each["UUID"] = str(uuid.uuid4())
                ri = RecipeInstructions(**each)
                recipe.recipeInstruction.append(ri.UUID)
                db_to_add.append({"tag": "RecipeInstruction", "node": ri})
        if "aggregateRating" in r_keys:
            recipe.ratingValue = r_data["aggregateRating"]["ratingValue"]
            recipe.reviewCount = r_data["aggregateRating"]["reviewCount"]
        if "recipeYield" in r_keys:
            recipe.recipeYield = r_data["recipeYield"]
        if "review" in r_keys:
            for each in r_data["review"]:
                if "author" in each:

                    each["author"]["UUID"] = str(uuid.uuid4())
                    a["UUID"] = "AUTHOR_" + each["author"]["name"]
                    r_author = Author(**each["author"])
                else:
                    a = {
                        "UUID": "ANONYMOUS_AUTHOR",
                        "name": "Anonymous"
                    }
                    r_author = Author(**a)

                r = {
                    "UUID": str(uuid.uuid4()),
                    "ratingValue": each["reviewRating"]["ratingValue"],
                    "worstRating": each["reviewRating"]["worstRating"],
                    "bestRating": each["reviewRating"]["bestRating"],
                    "author": r_author.UUID
                }
                review = Review(**r)
                recipe.review.append(review.UUID)
                db_to_add.append({"tag": "Author", "node": r_author})
                db_to_add.append({"tag": "Review", "node": review})
        if "video" in r_keys:
            a = r_data["video"]
            a["UUID"] = str(uuid.uuid4())
            vid = Video(**a)
            recipe.video = vid.UUID
            db_to_add.append({"tag": "Video", "node": vid})
        db_to_add.append({"tag": "Recipe", "node": recipe})
    return db_to_add


def crawl_webpages(webpage_urls):
    with DRIVER.session() as session:
        for url in webpage_urls:
            u = url.strip("\n")
            if not check_url_exists(u) and len(u) > 1:
                #delay = random.uniform(MIN_DELAY, MAX_DELAY)
                #delay = 3
                #print("Delaying crawl for: ", delay)
                # time.sleep(delay)
                print("Crawling: ", u)
                db_to_add = extract_data(u)

                # Create the nodes
                for each in db_to_add:
                    # Make sure not already exists
                    if not check_node_exists(each["node"].UUID):
                        # Make sure node isn't an author, for some reason...
                        if "name" in each["node"].__dict__:
                            if each["node"].name == "Anonymous":
                                each["node"].UUID = "ANONYMOUS_AUTHOR"
                        session.write_transaction(
                            insert_node, each["node"], each["tag"])
                    # Create relationships if necessary
                    if each["tag"] == "Recipe":
                        # Add author tags
                        if each["node"].author:
                            session.write_transaction(
                                insert_relationship, each["node"].UUID, each["node"].author, "AUTHORED_BY")
                        if each["node"].publisher:
                            session.write_transaction(
                                insert_relationship, each["node"].UUID, each["node"].publisher, "PUBLISHED_BY")
                        if each["node"].image:
                            for i in each["node"].image:
                                if "." not in i:  # Ignore url's
                                    session.write_transaction(
                                        insert_relationship, each["node"].UUID, i, "LINKS_IMAGE")
                        if each["node"].nutrition:
                            session.write_transaction(
                                insert_relationship, each["node"].UUID, each["node"].nutrition, "NUTRITIONAL_INFORMATION")
                        if each["node"].recipeInstruction:
                            for i in each["node"].recipeInstruction:
                                session.write_transaction(
                                    insert_relationship, each["node"].UUID, i, "RECIPE_INSTRUCTION")
                        if each["node"].review:
                            for i in each["node"].review:
                                session.write_transaction(
                                    insert_relationship, each["node"].UUID, i, "HAS_REVIEW")
                        if each["node"].video:
                            # for i in each["node"].video:
                            session.write_transaction(
                                insert_relationship, each["node"].UUID, each["node"].video, "LINKS_VIDEO")
                    elif each["tag"] == "Review":
                        session.write_transaction(
                            insert_relationship, each["node"].UUID, each["node"].author, "AUTHORED_BY")
            else:
                pass
                print("URL previously crawled, skipping")

# List of webpage URLs to crawl
#webpage_urls = ["https://www.foodnetwork.com/recipes/food-network-kitchen/4-ingredient-peaches-and-cream-pie-3364851"]


webpage_urls = []
with open("sitemap", "r") as f:
    webpage_urls = f.readlines()
# Crawl webpages and insert data into Neo4j
crawl_webpages(webpage_urls)

# Close the Neo4j driver
DRIVER.close()
