"""
ISIC Rev. 5 Taxonomy
Contains sections and divisions for the classification pipeline.
"""

SECTIONS = {
    "A": "Agriculture, forestry and fishing",
    "B": "Mining and quarrying",
    "C": "Manufacturing",
    "D": "Electricity, gas, steam and air conditioning supply",
    "E": "Water supply; sewerage, waste management and remediation activities",
    "F": "Construction",
    "G": "Wholesale and retail trade",
    "H": "Transportation and storage",
    "I": "Accommodation and food service activities",
    "J": "Publishing, broadcasting, and content production and distribution activities",
    "K": "Telecommunications, computer programming, consultancy, computing infrastructure, and other information service activities",
    "L": "Financial and insurance activities",
    "M": "Real estate activities",
    "N": "Professional, scientific and technical activities",
    "O": "Administrative and support service activities",
    "P": "Public administration and defence; compulsory social security",
    "Q": "Education",
    "R": "Human health and social work activities",
    "S": "Arts, sports and recreation",
    "T": "Other service activities",
    "U": "Activities of households as employers; undifferentiated goods- and services-producing activities of households for own use",
    "V": "Activities of extraterritorial organizations and bodies"
}

DIVISIONS = {
    "01": {
        "title": "Crop and animal production, hunting and related service activities",
        "section": "A",
        "full_description": "Agriculture, forestry and fishing - Crop and animal production, hunting and related service activities"
    },
    "02": {
        "title": "Forestry and logging",
        "section": "A",
        "full_description": "Agriculture, forestry and fishing - Forestry and logging"
    },
    "03": {
        "title": "Fishing and aquaculture",
        "section": "A",
        "full_description": "Agriculture, forestry and fishing - Fishing and aquaculture"
    },
    "05": {
        "title": "Mining of coal and lignite",
        "section": "B",
        "full_description": "Mining and quarrying - Mining of coal and lignite"
    },
    "06": {
        "title": "Extraction of crude petroleum and natural gas",
        "section": "B",
        "full_description": "Mining and quarrying - Extraction of crude petroleum and natural gas"
    },
    "07": {
        "title": "Mining of metal ores",
        "section": "B",
        "full_description": "Mining and quarrying - Mining of metal ores"
    },
    "08": {
        "title": "Other mining and quarrying",
        "section": "B",
        "full_description": "Mining and quarrying - Other mining and quarrying"
    },
    "09": {
        "title": "Mining support service activities",
        "section": "B",
        "full_description": "Mining and quarrying - Mining support service activities"
    },
    "10": {
        "title": "Manufacture of food products",
        "section": "C",
        "full_description": "Manufacturing - Manufacture of food products"
    },
    "11": {
        "title": "Manufacture of beverages",
        "section": "C",
        "full_description": "Manufacturing - Manufacture of beverages"
    },
    "12": {
        "title": "Manufacture of tobacco products",
        "section": "C",
        "full_description": "Manufacturing - Manufacture of tobacco products"
    },
    "13": {
        "title": "Manufacture of textiles",
        "section": "C",
        "full_description": "Manufacturing - Manufacture of textiles"
    },
    "14": {
        "title": "Manufacture of wearing apparel",
        "section": "C",
        "full_description": "Manufacturing - Manufacture of wearing apparel"
    },
    "15": {
        "title": "Manufacture of leather and related products",
        "section": "C",
        "full_description": "Manufacturing - Manufacture of leather and related products"
    },
    "16": {
        "title": "Manufacture of wood and of products of wood and cork, except furniture; manufacture of articles of straw and plaiting materials",
        "section": "C",
        "full_description": "Manufacturing - Manufacture of wood and of products of wood and cork, except furniture; manufacture of articles of straw and plaiting materials"
    },
    "17": {
        "title": "Manufacture of paper and paper products",
        "section": "C",
        "full_description": "Manufacturing - Manufacture of paper and paper products"
    },
    "18": {
        "title": "Printing and reproduction of recorded media",
        "section": "C",
        "full_description": "Manufacturing - Printing and reproduction of recorded media"
    },
    "19": {
        "title": "Manufacture of coke and refined petroleum products",
        "section": "C",
        "full_description": "Manufacturing - Manufacture of coke and refined petroleum products"
    },
    "20": {
        "title": "Manufacture of chemicals and chemical products",
        "section": "C",
        "full_description": "Manufacturing - Manufacture of chemicals and chemical products"
    },
    "21": {
        "title": "Manufacture of basic pharmaceutical products and pharmaceutical preparations",
        "section": "C",
        "full_description": "Manufacturing - Manufacture of basic pharmaceutical products and pharmaceutical preparations"
    },
    "22": {
        "title": "Manufacture of rubber and plastic products",
        "section": "C",
        "full_description": "Manufacturing - Manufacture of rubber and plastic products"
    },
    "23": {
        "title": "Manufacture of other non-metallic mineral products",
        "section": "C",
        "full_description": "Manufacturing - Manufacture of other non-metallic mineral products"
    },
    "24": {
        "title": "Manufacture of basic metals",
        "section": "C",
        "full_description": "Manufacturing - Manufacture of basic metals"
    },
    "25": {
        "title": "Manufacture of fabricated metal products, except machinery and equipment",
        "section": "C",
        "full_description": "Manufacturing - Manufacture of fabricated metal products, except machinery and equipment"
    },
    "26": {
        "title": "Manufacture of computer, electronic and optical products",
        "section": "C",
        "full_description": "Manufacturing - Manufacture of computer, electronic and optical products"
    },
    "27": {
        "title": "Manufacture of electrical equipment",
        "section": "C",
        "full_description": "Manufacturing - Manufacture of electrical equipment"
    },
    "28": {
        "title": "Manufacture of machinery and equipment n.e.c.",
        "section": "C",
        "full_description": "Manufacturing - Manufacture of machinery and equipment n.e.c."
    },
    "29": {
        "title": "Manufacture of motor vehicles, trailers and semi-trailers",
        "section": "C",
        "full_description": "Manufacturing - Manufacture of motor vehicles, trailers and semi-trailers"
    },
    "30": {
        "title": "Manufacture of other transport equipment",
        "section": "C",
        "full_description": "Manufacturing - Manufacture of other transport equipment"
    },
    "31": {
        "title": "Manufacture of furniture",
        "section": "C",
        "full_description": "Manufacturing - Manufacture of furniture"
    },
    "32": {
        "title": "Other manufacturing",
        "section": "C",
        "full_description": "Manufacturing - Other manufacturing"
    },
    "33": {
        "title": "Repair, maintenance and installation of machinery and equipment",
        "section": "C",
        "full_description": "Manufacturing - Repair, maintenance and installation of machinery and equipment"
    },
    "35": {
        "title": "Electricity, gas, steam and air conditioning supply",
        "section": "D",
        "full_description": "Electricity, gas, steam and air conditioning supply - Electricity, gas, steam and air conditioning supply"
    },
    "36": {
        "title": "Water collection, treatment and supply",
        "section": "E",
        "full_description": "Water supply; sewerage, waste management and remediation activities - Water collection, treatment and supply"
    },
    "37": {
        "title": "Sewerage",
        "section": "E",
        "full_description": "Water supply; sewerage, waste management and remediation activities - Sewerage"
    },
    "38": {
        "title": "Waste collection, treatment and disposal, and recovery activities",
        "section": "E",
        "full_description": "Water supply; sewerage, waste management and remediation activities - Waste collection, treatment and disposal, and recovery activities"
    },
    "39": {
        "title": "Remediation and other waste management service activities",
        "section": "E",
        "full_description": "Water supply; sewerage, waste management and remediation activities - Remediation and other waste management service activities"
    },
    "41": {
        "title": "Construction of residential and non-residential buildings",
        "section": "F",
        "full_description": "Construction - Construction of residential and non-residential buildings"
    },
    "42": {
        "title": "Civil engineering",
        "section": "F",
        "full_description": "Construction - Civil engineering"
    },
    "43": {
        "title": "Specialized construction activities",
        "section": "F",
        "full_description": "Construction - Specialized construction activities"
    },
    "46": {
        "title": "Wholesale trade",
        "section": "G",
        "full_description": "Wholesale and retail trade - Wholesale trade"
    },
    "47": {
        "title": "Retail trade",
        "section": "G",
        "full_description": "Wholesale and retail trade - Retail trade"
    },
    "49": {
        "title": "Land transport and transport via pipelines",
        "section": "H",
        "full_description": "Transportation and storage - Land transport and transport via pipelines"
    },
    "50": {
        "title": "Water transport",
        "section": "H",
        "full_description": "Transportation and storage - Water transport"
    },
    "51": {
        "title": "Air transport",
        "section": "H",
        "full_description": "Transportation and storage - Air transport"
    },
    "52": {
        "title": "Warehousing and support activities for transportation",
        "section": "H",
        "full_description": "Transportation and storage - Warehousing and support activities for transportation"
    },
    "53": {
        "title": "Postal and courier activities",
        "section": "H",
        "full_description": "Transportation and storage - Postal and courier activities"
    },
    "55": {
        "title": "Accommodation",
        "section": "I",
        "full_description": "Accommodation and food service activities - Accommodation"
    },
    "56": {
        "title": "Food and beverage service activities",
        "section": "I",
        "full_description": "Accommodation and food service activities - Food and beverage service activities"
    },
    "58": {
        "title": "Publishing activities",
        "section": "J",
        "full_description": "Publishing, broadcasting, and content production and distribution activities - Publishing activities"
    },
    "59": {
        "title": "Motion picture, video and television programme production, sound recording and music publishing activities",
        "section": "J",
        "full_description": "Publishing, broadcasting, and content production and distribution activities - Motion picture, video and television programme production, sound recording and music publishing activities"
    },
    "60": {
        "title": "Programming, broadcasting, news agency and other content distribution activities",
        "section": "J",
        "full_description": "Publishing, broadcasting, and content production and distribution activities - Programming, broadcasting, news agency and other content distribution activities"
    },
    "61": {
        "title": "Telecommunications",
        "section": "K",
        "full_description": "Telecommunications, computer programming, consultancy, computing infrastructure, and other information service activities - Telecommunications"
    },
    "62": {
        "title": "Computer programming, consultancy and related activities",
        "section": "K",
        "full_description": "Telecommunications, computer programming, consultancy, computing infrastructure, and other information service activities - Computer programming, consultancy and related activities"
    },
    "63": {
        "title": "Computing infrastructure, data processing, hosting, and other information service activities",
        "section": "K",
        "full_description": "Telecommunications, computer programming, consultancy, computing infrastructure, and other information service activities - Computing infrastructure, data processing, hosting, and other information service activities"
    },
    "64": {
        "title": "Financial service activities, except insurance and pension funding",
        "section": "L",
        "full_description": "Financial and insurance activities - Financial service activities, except insurance and pension funding"
    },
    "65": {
        "title": "Insurance, reinsurance and pension funding, except compulsory social security",
        "section": "L",
        "full_description": "Financial and insurance activities - Insurance, reinsurance and pension funding, except compulsory social security"
    },
    "66": {
        "title": "Activities auxiliary to financial service and insurance activities",
        "section": "L",
        "full_description": "Financial and insurance activities - Activities auxiliary to financial service and insurance activities"
    },
    "68": {
        "title": "Real estate activities",
        "section": "M",
        "full_description": "Real estate activities - Real estate activities"
    },
    "69": {
        "title": "Legal and accounting activities",
        "section": "N",
        "full_description": "Professional, scientific and technical activities - Legal and accounting activities"
    },
    "70": {
        "title": "Activities of head offices; management consultancy activities",
        "section": "N",
        "full_description": "Professional, scientific and technical activities - Activities of head offices; management consultancy activities"
    },
    "71": {
        "title": "Architectural and engineering activities; technical testing and analysis",
        "section": "N",
        "full_description": "Professional, scientific and technical activities - Architectural and engineering activities; technical testing and analysis"
    },
    "72": {
        "title": "Scientific research and development",
        "section": "N",
        "full_description": "Professional, scientific and technical activities - Scientific research and development"
    },
    "73": {
        "title": "Activities of advertising, market research and public relations",
        "section": "N",
        "full_description": "Professional, scientific and technical activities - Activities of advertising, market research and public relations"
    },
    "74": {
        "title": "Other professional, scientific and technical activities",
        "section": "N",
        "full_description": "Professional, scientific and technical activities - Other professional, scientific and technical activities"
    },
    "75": {
        "title": "Veterinary activities",
        "section": "N",
        "full_description": "Professional, scientific and technical activities - Veterinary activities"
    },
    "77": {
        "title": "Rental and leasing activities",
        "section": "O",
        "full_description": "Administrative and support service activities - Rental and leasing activities"
    },
    "78": {
        "title": "Employment activities",
        "section": "O",
        "full_description": "Administrative and support service activities - Employment activities"
    },
    "79": {
        "title": "Travel agency, tour operator, and other travel related activities",
        "section": "O",
        "full_description": "Administrative and support service activities - Travel agency, tour operator, and other travel related activities"
    },
    "80": {
        "title": "Investigation and security activities",
        "section": "O",
        "full_description": "Administrative and support service activities - Investigation and security activities"
    },
    "81": {
        "title": "Services to buildings and landscape activities",
        "section": "O",
        "full_description": "Administrative and support service activities - Services to buildings and landscape activities"
    },
    "82": {
        "title": "Office administrative, office support and other business support activities",
        "section": "O",
        "full_description": "Administrative and support service activities - Office administrative, office support and other business support activities"
    },
    "84": {
        "title": "Public administration and defence; compulsory social security",
        "section": "P",
        "full_description": "Public administration and defence; compulsory social security - Public administration and defence; compulsory social security"
    },
    "85": {
        "title": "Education",
        "section": "Q",
        "full_description": "Education - Education"
    },
    "86": {
        "title": "Human health activities",
        "section": "R",
        "full_description": "Human health and social work activities - Human health activities"
    },
    "87": {
        "title": "Residential care activities",
        "section": "R",
        "full_description": "Human health and social work activities - Residential care activities"
    },
    "88": {
        "title": "Social work activities without accommodation",
        "section": "R",
        "full_description": "Human health and social work activities - Social work activities without accommodation"
    },
    "90": {
        "title": "Arts creation and performing arts activities",
        "section": "S",
        "full_description": "Arts, sports and recreation - Arts creation and performing arts activities"
    },
    "91": {
        "title": "Library, archives, museum and other cultural activities",
        "section": "S",
        "full_description": "Arts, sports and recreation - Library, archives, museum and other cultural activities"
    },
    "92": {
        "title": "Gambling and betting activities",
        "section": "S",
        "full_description": "Arts, sports and recreation - Gambling and betting activities"
    },
    "93": {
        "title": "Sports activities and amusement and recreation activities",
        "section": "S",
        "full_description": "Arts, sports and recreation - Sports activities and amusement and recreation activities"
    },
    "94": {
        "title": "Activities of membership organizations",
        "section": "T",
        "full_description": "Other service activities - Activities of membership organizations"
    },
    "95": {
        "title": "Repair and maintenance of computers, personal and household goods, and motor vehicles and motorcycles",
        "section": "T",
        "full_description": "Other service activities - Repair and maintenance of computers, personal and household goods, and motor vehicles and motorcycles"
    },
    "96": {
        "title": "Personal service activities",
        "section": "T",
        "full_description": "Other service activities - Personal service activities"
    },
    "97": {
        "title": "Activities of households as employers of domestic personnel",
        "section": "U",
        "full_description": "Activities of households as employers; undifferentiated goods- and services-producing activities of households for own use - Activities of households as employers of domestic personnel"
    },
    "98": {
        "title": "Undifferentiated goods- and services-producing activities of private households for own use",
        "section": "U",
        "full_description": "Activities of households as employers; undifferentiated goods- and services-producing activities of households for own use - Undifferentiated goods- and services-producing activities of private households for own use"
    },
    "99": {
        "title": "Activities of extraterritorial organizations and bodies",
        "section": "V",
        "full_description": "Activities of extraterritorial organizations and bodies - Activities of extraterritorial organizations and bodies"
    }
}

# ---------------------------------------------------------------------------
# Rich Academic/Domain Enrichments for Embedding Model Classification
# ---------------------------------------------------------------------------
ENRICHED_DESCRIPTIONS = {
    "01": "farming, agricultural, crop, animal production, livestock, harvesting, cultivation, soil, irrigation, agronomy, farm management, sustainable agriculture, food security, agroecology.",
    "02": "forestry, logging, silviculture, timber, woodlands, forest conservation, deforestation, reforestation, tree planting, forest management.",
    "10": "food manufacturing, food processing, food science, nutrition, food safety, preservation, bakery, meat, dairy, milling, packaging, food quality.",
    "58": "publishing, books, journals, newspapers, academic publishing, literature, peer review, open access, editorial, printing, digital publishing, textbooks.",
    "59": "cinema, film, video production, broadcasting, motion pictures, movie, television, documentary, audio recording, media studies, sound engineering.",
    "62": "software development, computer programming, web application, software engineering, algorithms, coding, IT consultancy, systems integration, computer science, software architecture, mobile apps, database design, cybersecurity.",
    "63": "information systems, search engine, web portal, data processing, web hosting, digital library, data analytics, knowledge management, cloud computing, metadata, content management.",
    "69": "legal studies, law, court, litigation, contracts, jurisprudence, human rights, corporate law, accounting, auditing, bookkeeping, financial tax, accounting standards.",
    "70": "management consulting, business strategy, corporate governance, leadership, business administration, organizational behavior, public relations, strategic planning.",
    "71": "architectural design, structural engineering, civil engineering, drafting, urban planning, blueprints, technical testing, product design, engineering simulation.",
    "72": "scientific research, R&D, laboratory, experimental development, biotechnology, social science research, humanities research, experimental design, academic study, empirical research, methodology.",
    "73": "marketing, advertising, public relations, PR, market research, consumer behavior, brand management, survey design, focus groups, marketing strategy.",
    "74": "professional services, translation, interpreting, photography, graphic design, specialized design, consulting.",
    "84": "public administration, government policy, public policy, civil service, governance, public management, defense, social security, public finance, regulatory policy, international relations, policy analysis.",
    "85": "primary education, secondary education, higher education, university teaching, vocational training, technical education, adult education, tutoring, educational research, curriculum development, pedagogy, learning outcomes, student assessment, school systems, e-learning, distance learning, special needs education.",
    "86": "medical science, clinical research, hospital care, physician, nursing, diagnostics, therapy, pharmacology, public health, epidemiology, healthcare systems, mental health, psychiatry, primary care, patient outcomes.",
    "87": "nursing homes, residential care, elderly care, assisted living, hospice, rehabilitation, group homes, social care facilities.",
    "88": "social work, community services, counseling, child protection, family support, welfare assistance, humanitarian aid, disaster relief, social justice, vulnerable populations, non-profit organization.",
    "90": "performing arts, creative writing, theater, music, dance, visual arts, painting, sculpture, fine arts, artistic expression, literature.",
    "91": "library science, archives, museum, historical preservation, cultural heritage, curation, digital archives, galleries, historical documentation.",
    "93": "sports science, physical education, athletic training, recreation, leisure, fitness, sports coaching, sports management.",
    "94": "trade unions, professional associations, non-governmental organizations, NGOs, advocacy groups, civil society, interest groups."
}

# Update DIVISIONS with enriched descriptions to help the embedding model
for div_id, enrichment in ENRICHED_DESCRIPTIONS.items():
    if div_id in DIVISIONS:
        DIVISIONS[div_id]["full_description"] += f". Keywords: {enrichment}"

