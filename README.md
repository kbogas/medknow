# Medknow
This project is a submodule designed to be used in the creation of a disease specific knowledge base, but could be also used as a standalone module in other projects. It focuses on extracting **biomedical entities** and **relations** between them from free nlp text and structuring them in a way that makes extracting new knowledge and infering hidden relations easier, utiling a **graph** database.

This project has been designed with modularity in mind, allowing the implementation and fast integration of new extractors, such as [ReVerb](http://reverb.cs.washington.edu/) for relation extraction and [MetaMap](https://metamap.nlm.nih.gov/) for concept extractions. Those two are currently being developed alongside the already implemented extractor based on [SemRep](https://semrep.nlm.nih.gov/).

Currently the main features of this project are(some under work):
* Different kind of input sources: free text, already extracted relations, concepts etc.
* A variety of knowledge extractors working in a pipeline: **SemRep**, **MetaMap**, **Reverb**
* Multiple persistency options: saving enriched documents to file, entities and relations to .csv, utilizing **Neo4j** 

## Getting Started
These instructions will get you a copy of the project up and running on your local machine for development and testing purposes.

### Knowledge Extraction
This project is based around concept and relation extractions from text [SemRep](https://semrep.nlm.nih.gov/). Follow, instrunctions on their website in order to set-up a copy on your local machine.
**Note**: You will have to also install **MetaMap** for SemRep to work.

### Neo4j
If you'd like to persist results in Neo4j you will have to pre-install in on your local machine. More details available in their [website](https://neo4j.com/).

### Python Modules
This is pretty straightforward, the needed modules are located in *requirements.txt*. You can, either install them individually or better yet use [pip](https://pip.pypa.io/en/stable/) to install them in a bundle, by executing:
```
pip install -r requirements.txt
```
after clonining/downloading the project folder.
(Maybe, asked for admin/sudo rights)

### Using
The functionalities offered by this module are wrapped in a **Pipeline**, broken down into three phases. These are:
1. *Input*: What type of input do we expect to deal with? Where to read it from? Specific fields in .csv or .json files that we must take into account etc.
2. *Transformations*: What type of transformations to do the input provided? Enrich the input document with concepts and relations using SemRep? MetaMap + Reverb? Transform an edge-list between already existing entities into the correct structure for populating the Neo4j db? 
3. *Output*: What to do with the results? Save enriched file in .json? Output .csv files for use by the neo4j import-tool? Directly create/update the Neo4j db?

All of these choices are paramaterized in **settings.yaml**, following the .yaml structure. The outline of the available parameters is the following:
- **pipeline**: Mainly *True/False* values for specific keys regarding the previously presented phases, denoting what functions to be completed.
- **input**: Variables regarding the paths of SemRep, input files, as well as, key fields in .json and .csv files where text and other information is stored
- **API Keys**: API keys used for specific services.
- **Neo4j**: Details regarding the connection to an existing Neo4j instance
- **Output**: Variables and paths regarding the generated results.

Details on each variable is found in the settings.yaml.
#### !!!! CONFIGURE SETTINGS.YAML BEFORE RUNNING THE SCRIPT !!!!
Finally, after configuration to match your needs simply run:

```python
python run.py
```
## Tests

Currently no tests supported.

## Questions/Errors

Bougiatiotis Konstantinos, NCSR ‘DEMOKRITOS’ E-mail: bogas.ko@gmail.com
