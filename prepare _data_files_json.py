
import json
import ast


meta_file='baby/meta_Baby.json'
meta_json_file='baby/meta_json_Baby.json'
prod_text_json_file='baby/prod_text_json_Baby.json'
prod_no_relation_json_file='baby/prod_no_relation_json_Baby.json'

# preparing meta data - title - description
print("preparing meta data - title - description")
meta_json_file_target = open(meta_json_file, 'w')
prod_text_json_file_target = open(prod_text_json_file, 'w')
prod_no_relation_json_file_target = open(prod_no_relation_json_file, 'w')
with open(meta_file) as f:
    for line in f:
        prod_meta_data = {}
        json_data=ast.literal_eval(line)
        prod_meta_data["asin"]=json_data["asin"]
        prod_meta_data["brand"]=json_data.get("brand",'')
        prod_meta_data["price"]=json_data.get("price",0)
        # add title and description to reviews
        prod_text_data = {}
        product_title=json_data.get("title",'')
        product_desc=json_data.get("description",'')
        prod_meta_data_title_len = len(product_title)
        prod_meta_data_desc_len = len(product_desc)
        prod_meta_data_title_desc = (' ' if prod_meta_data_title_len > 0 else '') + product_title + (' ' if prod_meta_data_desc_len > 0 else '') + product_desc
        if len(prod_meta_data_title_desc) > 0:
            prod_text_data["asin"]=json_data["asin"]
            prod_text_data["text"] = " ".join([prod_meta_data_title_desc])

        ## also viewed also bought
        has_also_viewed = False
        has_also_bought = False

        related = json_data.get("related", [])
        if len(related) > 0:
            also_viewed = related.get("also_viewed", [])
            has_also_viewed = len(also_viewed) > 0
            if has_also_viewed:
                prod_meta_data["also_viewed"] = also_viewed

            also_bought = related.get("also_bought", [])
            has_also_bought = len(also_bought) > 0
            if has_also_bought:
                prod_meta_data["also_bought"] = also_bought
            # if has no also viewed or also bought ==> has no relation
            if not has_also_viewed and not has_also_bought:
                prod_no_relation_data = {}
                prod_no_relation_data["no_relation"] = prod_meta_data["asin"]
                prod_no_relation_json_file_target.write(json.dumps(prod_no_relation_data))
                prod_no_relation_json_file_target.write('\n')

            #writing to files
            #meta data
            meta_json_file_target.write(json.dumps(prod_meta_data))
            meta_json_file_target.write('\n')
            #prod text
            prod_text_json_file_target.write(json.dumps(prod_text_data))
            prod_text_json_file_target.write('\n')

        ## has no relation
        else:
            prod_no_relation_data = {}
            prod_no_relation_data["no_relation"]=prod_meta_data["asin"]
            prod_no_relation_json_file_target.write(json.dumps(prod_no_relation_data))
            prod_no_relation_json_file_target.write('\n')
            # writing to files
            # meta data
            meta_json_file_target.write(json.dumps(prod_meta_data))
            meta_json_file_target.write('\n')
            # prod text
            prod_text_json_file_target.write(json.dumps(prod_text_data))
            prod_text_json_file_target.write('\n')

            continue

meta_json_file_target.close()
prod_no_relation_json_file_target.close()

# preparing reviews
print("preparing reviews")
reviews_file='baby/reviews_Baby_5.json'

with open(reviews_file) as f:
    for line in f:
        prod_reviews={}
        json_data = ast.literal_eval(line)
        prod_reviews["asin"]=json_data["asin"]
        prod_reviews["text"]=json_data["reviewText"]
        prod_text_json_file_target.write(json.dumps(prod_reviews))
        prod_text_json_file_target.write('\n')


qa_file='baby/qa_Baby.json'

# preparing QAs
print("preparing QAs")
with open(qa_file) as f:
    for line in f:
        prod_qa = {}
        json_data = ast.literal_eval(line)
        prod_qa["asin"]=json_data["asin"]
        prod_qa["text"]=" ".join([json_data.get("question",''),json_data.get("answer",'')])
        prod_text_json_file_target.write(json.dumps(prod_qa))
        prod_text_json_file_target.write('\n')


prod_text_json_file_target.close()

