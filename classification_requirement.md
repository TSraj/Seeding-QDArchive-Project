Part 2: Classification

Step 1
Filter all projects into one of
QDA_PROJECT
QD_PROJECT
OTHER_PROJECT
NOT_A_PROJECT
Add this information to a new column “type” of type PROJECT_TYPE

Use the file types to derive the PROJECT_TYPE
QDA_PROJECT if there is a file with QDA file extension
QD_PROJECT if not a QDA_PROJECT and there are primary data files
OTHER_PROJECT if not a QD_PROJECT and there are valid data files
NOT_A_PROJECT if nothing can be derived about file types

Step 2
Develop a classifier for the available data
Uses both the base data (the file) and the metadata
Use the ISIC Rev. 5 standard for a hierarchical classification taxonomy
Go down two levels, i.e. divisions (not just sections)
Also consider creating tags for searching

Step 3
Run the classifier on your data two times
Once for QDA_PROJECT types of project
Classify the project (as the sum of its files) itself
Classify each primary data file as well
Once for QD_PROJECT types of project
Classify the project (as the sum of its files) itself
Classify each primary data file as well

Step 4
Report about the resulting statistics
How much qualitative data was found
Number of projects by PROJECT_TYPE
The distribution of sections and divisions in the data

Step 5
Report about technical challenges with data (not with programming)


---

## Questions Asked & Professor's Answers (May 2026)

### Q1. Technical approach for the ISIC Rev. 5 classifier
**Asked:** What approach — LLM, embedding-based, or keyword/rule-based?
**Answer:** "Your choice, whatever works for you."
**Decision:** Use sentence-transformers (embedding-based) — free, local, reproducible. No paid APIs.

### Q2. Depth of base data extraction
**Asked:** Should we use the tiered extraction strategy (Tier 1: metadata, Tier 2: text files, Tier 3: excluded)?
**Answer:** "Certainly use your tier 1 data (i.e. metadata). Also use and focus on your tier 2 data i.e. the actual content i.e. primary data files; don't forget that in a QDA file (which are often just zip files) there may be primary data included."
**Decision:** Use Tier 1 + Tier 2. Also extract text from inside QDA archives (unzip). Tier 3 (audio/video/images) relies on metadata only.

### Q3. Validation and evaluation of classifier quality
**Asked:** Do we need a labeled validation set or accuracy metrics?
**Answer:** "The work is purely descriptive."
**Decision:** No accuracy evaluation needed. Report distributions only.

### Q4. File extension definitions
**Asked:** Confirm lists for QDA files, primary data files, and valid data files.
**Answer:** "Whatever you can parse; Python should give you easy access to txt, docx, etc."
**Decision:** Use our proposed lists (see implementation plan for full details).

### Q5. ISIC source & deliverable format
**Asked:** Which ISIC Rev. 5 source? What report format?
**Answer:** "See class discussion, slides and notes." ISIC ref: http://unstats.un.org/unsd/classifications/Econ/
**Decision:** Use official UN Statistics Division ISIC Rev. 5. Report format TBD (professor will clarify in future classes).

---

## Additional Requirements from Slides (May 2026)

### Reporting Structure
Results must be broken down by **repository × project type**:
- There are 9 repositories in the database
- 2 classifiable project types: QDA_PROJECT, QD_PROJECT
- Total: **9 × 2 = 18 separate distributions**
- Results should NOT be aggregated across repositories or project types

### Report Contents (when format is finalized)
- How much qualitative data was found
- Number of projects by PROJECT_TYPE and repository
- The distribution of sections and divisions in the data
- Submit a link to a Google Spreadsheet and a PDF
- One tab contains the distribution data and a histogram of the data
- (Report format details still pending — professor will clarify in future classes)

---

## Key Constraint
**FREE RESOURCES ONLY** — no paid API keys (OpenAI, Gemini, Claude, etc.). All tools must be free and run locally.
