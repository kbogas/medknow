# Medknow
This project is a submodule designed to be used in the creation of a disease-specific knowledge base, but could be also used as a standalone module in other projects. It focuses on extracting **biomedical entities** and **relations** between them from free nlp text and structuring them in a way that makes extracting new knowledge and inferring hidden relations easier, utilizing a **graph** database.

This project has been designed with modularity in mind, allowing the implementation and fast integration of new extractors, such as [ReVerb](http://reverb.cs.washington.edu/) for relation extraction and [MetaMap](https://metamap.nlm.nih.gov/) for concept extractions. Those two are currently being developed alongside the already implemented extractor based on [SemRep](https://semrep.nlm.nih.gov/).

Currently, the main features of this project are(some under work):
* Different kind of input sources: free text, already extracted relations, concepts etc.
* A variety of knowledge extractors working in a pipeline: **SemRep**, **MetaMap**, **Reverb**
* Multiple persistency options: saving enriched documents to file, entities and relations to .csv, utilizing **Neo4j** 

## Getting Started
These instructions will get you a copy of the project up and running on your local machine for development and testing purposes.

### Knowledge Extraction
This project is based around concept and relation extractions from text [SemRep](https://semrep.nlm.nih.gov/). Follow, instructions on their website in order to set-up a copy on your local machine.
**Note**: You will have to also install **MetaMap** for SemRep to work.

### Neo4j
If you'd like to persist results in Neo4j you will have to pre-install in on your local machine. More details available on their [website](https://neo4j.com/).

### Python Modules
This is pretty straightforward, the needed modules are located in *requirements.txt*. You can, either install them individually or better yet use [pip](https://pip.pypa.io/en/stable/) to install them in a bundle, by executing:
```
pip install -r requirements.txt
```
after cloning/downloading the project folder.
(Maybe, asked for admin/sudo rights)

### Usage
The functionalities offered by this module are wrapped in a **Pipeline**, broken down into three phases. These are:
1. *Input*: What type of input do we expect to deal with? Where to read it from? Specific fields in .csv or .json files that we must take into account etc.
2. *Transformations*: What type of transformations to do the input provided? Enrich the input document with concepts and relations using SemRep? MetaMap + Reverb? Transform an edge-list between already existing entities into the correct structure for populating the Neo4j db? 
3. *Output*: What to do with the results? Save enriched file in .json? Output .csv files for use by the neo4j import-tool? Directly create/update the Neo4j db?

All of these choices are parameterized in **settings.yaml**, following the .yaml structure. The outline of the available parameters is the following:
- **pipeline**: Mainly *True/False* values for specific keys regarding the previously presented phases, denoting what functions to be completed.
- **input**: Variables regarding the paths of SemRep, input files, as well as, key fields in .json and .csv files where text and other information is stored
- **API Keys**: API keys used for specific services.
- **Neo4j**: Details regarding the connection to an existing Neo4j instance
- **Output**: Variables and paths regarding the generated results.

Details on each variable are found in the settings.yaml. An overview of the available keys-values is presented here:
**pipeline**:
 - *inp*:  What kind of input to we expect. Will specify what part of the 'load' section to read from. Currently supporting the following values:
    - **json**: Used for json from the harvester and enriched jsoni generated from this module.
    -  **edges**: A field containing edges-relations is expected to be found in the file. Used for DOID,DRUGBANK,MESH etc. relations.
    -  **med_rec**: Would be used for medical records but the main functionality is that it deals with delimited-files.
- *trans*: What kind of transformations-extractions to do:
    - **metamap**: True/False. If we want to extract entities using metamap. TODO: ! MERGE Entities and Treshold ! 
    - **reverb**: True/False. If we want to extract relations using reverb. TODO: ! Map Entities to UMLS CONCEPTS IN SENTENCE!
    - **semrep**: True/False. The main functionality. If we want to use SEMREP to extract relations and entities from text. !! It is meaningful only for json and med_rec, as edges are not supposed to have text field. !!
    - **get_concepts_from_edges**: True/False. ! This is for edges file only ! If we want some kind of transformation to be done in the entities found as subjects-objects in the edges file (e.g. fectch concepts from cuis, from DRUGBANK unique ids etc.)
- *out*: Where to write the output
    - **json**: True/False. Save the intermediate json generated after all the transformations/extraction are done, before updating the database.
    - **csv**: True/False. Create the corresponding node and edge files, to be used by the command-line neo4j import-tool. Not very useful for the time being.
    - **neo4j**: True/False. Create/Update the neo4j graph with the entities and relations found in the json generated from the trans steps or the **pre-enriched** json of 'json' or 'edges' input given at the start.

**load**:
  - *path*:
    - **metamap**: Path to metamap binary.*
    - **reverb**: Path to reverb binary.*
    - **semrep**: Path to semrep binary.*
  - *med_rec*: If the value in pipeline 'inp' is not **med_rec** the following values are irrelevant for the task at hand.
    - **inp_path**: Path to delimited file.*
    - **textfield**: Name of the column where the text is located (e.g. MedicalDiagnosis).*
    - **sep**: Delimiter value (e.g. \t).*
    - **idfield**: Name of the column where the ids are found (e.g. patient_id).*
  - *json*: If the value in pipeline 'inp' is not **json** the following values are irrelevant for the task at hand.
    - **inp_path**: Path to json file.*
    - **docfield**: Outer field of the json file where the documents/articles are located (e.g. documents).*
    - **textfield**: Name of the field to read text from (e.g. abstractText).*
    - **idfield**: Name of the column where the ids are found (e.g. pmid.*
    - **labelfield**: Field where the label of the document is situated (e.g. title).*
- *edges*: If the value in pipeline 'inp' is not **edges** the following values are irrelevant for the task at hand.
    - **inp_path**: Path to edges file.*
    - **edge_field**: Name of the outer field where the relations-edges are found (e.g. relations).*
    - **sub_type**:Type of the subject in the relations. Currently supporting Entity, Article and any new type of nodes.*
    - **obj_type**:Type of the pbject in the relations. Currently supporting Entity, Article and any new type of nodes.*
    - **sub_source**: What type of source is needed to transform the subject entity. Currently supporting: UMLS when the entities are cuis, [MSH, DRUGBANK, .. and the rest from the umls rest mapping used accordingly], [Article, Text and None when no transformation is needed on the subject entity given].*
    - **obj_source**: What type of source is needed to transform the object entity. Currently supporting: UMLS when the entities are cuis, [MSH, DRUGBANK, .. and the rest from the umls rest mapping used accordingly], [Article, Text and None when no transformation is needed on the object entity given].*

**apis**: API Keys for when calling different services
  - **biont**: Bioportal api for fetching uri info of a concept. Not currently in use.*
  - **umls**: UMLS REST api key. Useful only when the 'inp' in pipeline is **edges** and **get_concepts_from_edges** is True.*

**neo4j**: Variables for connection to an existing and running neo4j graph. If **neo4j** is False in the pipeline the following don't matter.
  - **host**: Database url (e.g localhost).*
  - **port**: Port number (e.g. 7474).* 
  - **user**: Username (e.g. neo4j).*
  - **password**: Password (e.g. admin).*
 
**out**: Which of the following sections will be used is related to whether the corresponding key in the pipeline 'out' field has a True value. If not, they don't matter.
- *json*:
    - **out_path**: path where the generated json will be saved.* 
    - **json_doc_field**: Name of the outer field containing the enriched-transformed articles-relations (e.g. mostly documents or  relations till now, according to whether we have 'json'(articles) or 'edges'(relations) to process). Better use the same as in the 'edges' or 'json' outerfield accordingly.*
    - **json_text_field**: For 'articles' or input that has text, the name of the field to save the text to (e.g. text).*
    - **json_id_field:** For 'articles' or collection of documents, the name of the field to save their id (e.g. id).*
    - **json_label_field**: For 'articles' or collection of documents, the name of the field to save their label (e.g. title).*
    - **sent_prefix**: For 'articles' or input that has text, the prefix to be used in the sentence-id generation procedure (e.g. abstract/fullbody).*
- *csv*:
    - **out_path**: path where the nodes and edges .csvs will be saved.* 
- neo4j:
    - **out_path**: This is just for printing purposes that the save will be perfomed in 'out_path'. Change the variables in the **neo4j** section if you want to configure access to neo4j, not this! (e.g. localhost:7474)*

The fields ending with *, can take up any value if they are not needed in the corresponding task. The pipeline denotes which tasks to do and correspondiglym which sections of the .yaml to access.



#### !!!! CONFIGURE SETTINGS.YAML BEFORE RUNNING THE SCRIPT !!!!
Finally, after configuration to match your needs simply run:

```python
python test.py
```
## Tests

Currently no tests supported.

## Questions/Errors

Bougiatiotis Konstantinos, NCSR ‘DEMOKRITOS’ E-mail: bogas.ko@gmail.com