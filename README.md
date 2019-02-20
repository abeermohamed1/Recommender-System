# Recommender-System
Implementation of Inferring Networks of Substitutable and Complementary Products Model paper


In this project we are implementing ready recommender model. The model is provided by Julian McAuley, Rahul Pandey, and Jure Leskovec. They develop a method to infer networks of substitutable and complementary products. The paper title is [“Inferring Networks of Substitutable and Complementary Products”](https://arxiv.org/abs/1506.08839). Our contribution here that we implement this model on Spark and we deployed it using Graphx; in addition to different data preparation approach.

We apply the recommender model on [Amazon data](http://jmcauley.ucsd.edu/data/amazon/) for baby. for details about data format and structure. please refer to http://jmcauley.ucsd.edu/data/amazon/

The project has 7 main phases

* **Data Understanding Phase:**

  We used a data extracted from amazon (Baby products data).
  Three files:
  1-	meta_Baby contains the main data about the product

  2-	qa_Baby contains questions and answers about the products

  3-	reviews_Baby_5 contains the reviews written by users on the product

  Notes: 

  1-	Some data is incomplete, some products has no text.

  2-	Merging all the text will give a well sized text for the product
  so we merged description, title, questions, answers and reviews for each product to form a one large text for each product.

* **[Data Preparation Phase](https://github.com/abeermohamed1/Recommender-System/blob/master/prepare%20_data_files_json.py):**
**(prepare _data_files_json.py)**

  1-	The Files we got are in the Dict python serialization format So first we Changed them into JSON format contains fields needed:
  
  2-	All the text aggregated into one large Doc for each product
  
  3-	Products has null or zero length doc are filtered out
  
  4-	Create the relations depending on also viewed and also bought data
  
  5-	Replicate some no related data size of products that has no also viewed nor also bought data is very small (just 10 %) we created a not related products data from those products
  Goals achieved by this step
  1-	Now data is ready for the Text Mining step
  
  2-	extra not used data in our algorithm are filtered out from the data
  
  3-	empty useless products (has no reviews nor description ) are filtered out

* **[Model Phase](https://github.com/abeermohamed1/Recommender-System/blob/master/FinalProject_jar):**
**(FinalProject_jar)**

  1-	Text mining phase:
    a.	Text tokenization
    b.	Remove stop words
    c.	Stemming the words
    d.	Removing empty words or words whose length is just one character
    e.	Removing words that are just digits
    f.	Doing count vectorization on the text

  2-	LDA phase: (Latent Dirichlet Allocation)
  This is done on the text to get the most k (k= 9 in our model) common topics in the reviews and get its percent of representation in each product text

  3-	We used the also viewed and also bought data to detect if there is a relation between products and its type

  4-	Preparation step before Logistic regression
  First
  the result of the LDA is a vector of the probability of occurrence of each topic in the text of the product
  For every two related products
    
    a.	We multiply the 2 vectors produced from the LDA 
    if they have the same topics the result of the multiplication will not differ much than the original data of every product. So they represent the same topics so they are mostly of the same category
    if they have different topic representation or some topics are represented in one product and not on the other, the result of the multiplication will result in very small values which may be zero. So those products are probably not in the same category
    
    b.	We subtracted the 2 vectors produced from the LDA after adding the price of each in the vectors, also we added a value for the new resulted vector representing if they have the same company or (0.1 and 0.9 respectively)
    this will give us a direction if prod1 will be recommended for prod2 or vice versa

  Second we used some products which has no relations to randomly create some not related products 
  Third we marked the also viewed related products as supplementary and the also bought as complementary products and used the multiplication result for them


* **Logistic regression Phase:**
**(compsup_logit.py, relation_logit.py)**

  We trained logistic regression models to use in future predictions of new products
  we trained 2 models 

    a.	First one to predict if there is a relation between the 2 products to predict the relation (weight and direction) 

      i.	we trained a logistic regression model on the multiplication result 

      ii.	we trained a logistic regression model on the subtraction result

      iii.	we multiplied the two logistic regression prediction results. The resulted value will detect the strength of the relation and the sign of the value (+ve or –ve) will detect the direction of the relation

    b.	Second one is to predict the relation type (complementary or supplementary) 

* **[Graph creation phase](https://github.com/abeermohamed1/Recommender-System/blob/master/Search.scala):**
**(Search.scala,Graphx.scala)**

  We created a graph of the related products to use in predictions
  The edge:
  
  a.	is weighted to detect the strength of the relation between the product
  
  b.	carrying the relation type (complementary or supplementary)
  
  NOTE : the graph is directed from one product to the other to detect the direction of recommendation e.g. you may recommend the charger as a complementary to a laptop but can\not do the opposite to detect this direction we used the price and the company similarity we may recommend the cheaper and the one from another company.

* **Model Evaluation:**

  The model evaluation depends on training data mentioned in previous section as following
  1-	The model studied against training data with number of iteration 100 to predict relation weight 
  
  2-	Set a threshold on predicted relation weight  to map it to Boolean format describes if it is related or not
  
  3-	compare the result with the given labels and get training error by counting non matched using following equation
  train-error = count (label != pred)/count(lables)

* **Deployment Phase:**

  The best way to represent the relations between related products; is to represent them as a graph. 

for more details please review the [documentation](https://github.com/abeermohamed1/Recommender-System/blob/master/Recommender%20System%20-%20Documentation.pdf) and [Installation Guide](https://github.com/abeermohamed1/Recommender-System/blob/master/Installation%20Guide.txt)
Hope this would be helpfull for every one
