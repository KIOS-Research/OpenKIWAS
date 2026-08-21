<h3>README – OpenKIWAS Knowledge Base (NotebookLM)</h3>
<h4>Purpose of This Notebook</h4>
<p>This NotebookLM environment contains structured textual representations of catalogues developed within the OpenKIWAS knowledge base. The catalogues support the exploration, design, and evaluation of inland water digital twin applications by providing curated information spanning policies, standards, projects, scientific results, tools, and data sources.
This README also serves as an instruction layer for retrieval-augmented generation (RAG) and defines how the language model should interpret and use the provided sources.</p>
<hr>
<h4>Rules for Retrieval and Answer Generation</h4>
<h5>Use only the provided sources</h5>
<p>All answers must be grounded exclusively in the catalogue entries contained in the uploaded source files.</p>
<h5>Do not invent information</h5>
<p>Do not introduce projects, datasets, tools, policies, standards, methods, or relationships that are not explicitly supported by the sources.</p>
<h5>State absence explicitly</h5>
<p>If relevant information cannot be found in the sources, respond with:
<strong>“Not found in OpenKIWAS sources.”</strong></p>
<h5>Reference specific entries</h5>
<p>When listing or discussing items, include the corresponding  <strong>ENTRY ID</strong>  and  <strong>TITLE</strong>  exactly as they appear in the sources.</p>
<h5>Do not infer relationships unless stated</h5>
<p>Links between catalogues (e.g. between a project and a dataset) should only be described if they are explicitly mentioned in the entries.</p>
<h5>Respect catalogue scope</h5>
<p>Interpret questions and generate answers in line with the thematic scope of each catalogue as described below.</p>
<hr>
<h4>Catalogue Descriptions</h4>
<h5>Task_3.1.1_Policies</h5>
<p>This catalogue contains policy instruments and regulatory frameworks relevant to inland water management at European and international levels. It includes directives, regulations, strategies, and policy documents that influence governance, compliance requirements, and decision-making for inland water systems and digital twin applications.</p>
<h6>Catalogue Structure – Policies (Entry Fields)</h6>
<p>This catalogue is represented as a set of  <em>entries</em>  (one entry per policy instrument). Each entry contains a consistent set of fields extracted from the original Excel columns and converted into text packs for NotebookLM. The fields are interpreted as follows:</p>
<ul>
<li><strong>ID</strong>
Unique identifier of the policy entry within OpenKIWAS.</li>
<li><strong>Title</strong>
Official title or commonly used name of the policy instrument.
<em>(Note: this corresponds to the original Excel column “Name”, which is normalised to “Title” in the text-pack export.)</em></li>
<li><strong>Issuing organisation</strong>
Institution or authority responsible for issuing/adopting the policy (e.g., EU body, national ministry, international organisation).</li>
<li><strong>Geographical Scope</strong>
Jurisdiction or spatial coverage to which the policy applies (e.g., EU-wide, national, regional, international).</li>
<li><strong>Type</strong>
Category of the policy instrument (e.g., directive, regulation, strategy, framework, guideline).</li>
<li><strong>Scientific domain</strong>
Primary scientific or technical domain addressed (e.g., hydrology, water quality, climate adaptation, environmental protection).</li>
<li><strong>Type of water</strong>
Inland/coastal/transitional water categories explicitly covered by the policy (e.g., rivers, groundwater, reservoirs, coastal/transitional waters).</li>
<li><strong>Water management focus</strong>
Main objectives and themes addressed (e.g., flood risk management, water quality protection, allocation, conservation, governance).</li>
<li><strong>Parameter change rate assumption</strong>
The policy’s assumptions about how quickly relevant parameters evolve over time. This field may include structured subfields:
<ul>
<li><strong>Parameter</strong> : the key regulated/monitored variables or indicators referenced by the policy (e.g., ecological status, discharge thresholds, quality parameters).</li>
<li><strong>Change rate</strong> : indicative speed of change of those parameters (e.g., slow-changing/static vs fast-changing/dynamic), as represented in the catalogue.</li>
</ul>
</li>
<li><strong>Description</strong>
Short summary of the policy’s scope, intent, and relevance to inland water management and digital twin development.</li>
<li><strong>Status</strong>
Current status of the policy (e.g., active, repealed, under revision, proposed), as captured in the catalogue entry.</li>
<li><strong>Effect Date</strong>
Date the policy entered into force or became applicable.</li>
<li><strong>Resource</strong>
Reference to the authoritative policy source (e.g., official document link, registry record).</li>
<li><strong>Recommendations</strong>
Guidance or suggested actions derived from the policy that may inform compliance, implementation, or system design decisions.
<strong>Typical use:</strong></li>
<li>Identifying regulatory and governance contexts</li>
<li>Understanding policy drivers and constraints</li>
<li>Mapping policy requirements to technical or data considerations</li>
</ul>
<hr>
<h5>Task_3.1.1_Standards</h5>
<p>This catalogue includes technical, data, and interoperability standards relevant to inland water systems, digital twins, and cyber-physical infrastructures. It covers international, European, and sector-specific standards guiding system design, data exchange, modelling practices, and interoperability.</p>
<h6>Catalogue Structure – Standards (Entry Fields)</h6>
<p>This catalogue is represented as a set of  <em>entries</em> , where each entry corresponds to a technical, data, or interoperability standard relevant to inland water systems and digital twin applications. The entries are derived from the original Excel columns and converted into structured text packs for use in NotebookLM. The fields are interpreted as follows:</p>
<ul>
<li><strong>ID</strong>
Unique identifier of the standard entry within the OpenKIWAS catalogue.</li>
<li><strong>Title</strong>
Official title or commonly used name of the standard.
<em>(Note: this corresponds to the original Excel column “Name”, which is normalised to “Title” in the text-pack export.)</em></li>
<li><strong>Issuing organisation</strong>
Organisation or standards body responsible for publishing or maintaining the standard (e.g., ISO, CEN, OGC, IEC, WMO, national standards bodies).</li>
<li><strong>Geographical Scope</strong>
Spatial or jurisdictional scope in which the standard is applicable (e.g., international, European, national, sector-specific).</li>
<li><strong>Type</strong>
Classification of the standard (e.g., data standard, interoperability standard, modelling standard, communication protocol, best-practice guideline).</li>
<li><strong>Water management focus</strong>
Inland water management aspects addressed by the standard (e.g., water quality monitoring, hydrological data exchange, infrastructure operation, interoperability).</li>
<li><strong>Description</strong>
Short textual summary describing the purpose, scope, and relevance of the standard in the context of inland water systems and digital twins.</li>
<li><strong>Status</strong>
Current lifecycle status of the standard as captured in the catalogue (e.g., active, effective, superseded, under revision, draft).</li>
<li><strong>Effect Date</strong>
Date on which the standard became effective, was published, or entered into force.</li>
<li><strong>Resource</strong>
Reference to the authoritative source of the standard (e.g., standards registry entry, official publication link).
<strong>Typical use:</strong></li>
<li>Identifying applicable standards for architecture and data models</li>
<li>Supporting interoperability and integration</li>
<li>Informing implementation best practices</li>
</ul>
<hr>
<h5>Task_3.1.2_Projects</h5>
<p>This catalogue contains research and innovation projects funded under European and related programmes (e.g. FP7, Horizon 2020, Horizon Europe) addressing inland water management, digital twins, modelling, platforms, and decision-support systems.</p>
<h6>Catalogue Structure – Projects (Entry Fields)</h6>
<p>This catalogue is represented as a set of  <em>entries</em> , where each entry corresponds to a research or innovation project relevant to inland water systems, digital twins, modelling, platforms, or decision-support applications. Entries are derived from project metadata and converted into structured text packs for use in NotebookLM. The fields are interpreted as follows:</p>
<ul>
<li><strong>ID</strong>
Unique identifier of the project entry within the OpenKIWAS catalogue.</li>
<li><strong>fiwar</strong>
Internal or project-specific classification flag as captured in the original source.
<em>(The semantic meaning of this field depends on the originating dataset and should be interpreted only as recorded in the entry.)</em></li>
<li><strong>Project_Acronym</strong>
Official acronym used to identify the project in funding programmes, publications, and dissemination materials.</li>
<li><strong>Title</strong>
Full official title of the project.
<em>(Note: this corresponds to the original Excel column “Project_Name”, which is normalised to “Title” in the text-pack export.)</em></li>
<li><strong>Funding Authority</strong>
Organisation or programme providing funding for the project (e.g., European Commission, national funding body).</li>
<li><strong>Call for proposals</strong>
Specific funding call or programme under which the project was funded (e.g., FP7 call, Horizon 2020 topic, Horizon Europe destination).</li>
<li><strong>Focus Area</strong>
Primary thematic or technical focus of the project (e.g., digital twins, hydrological modelling, water management, decision support, sensing).</li>
<li><strong>Begin Date</strong>
Official project start date.</li>
<li><strong>End Date</strong>
Official project end date, if completed, or planned end date for ongoing projects.</li>
<li><strong>Status</strong>
Current project status (e.g., completed, ongoing, planned), as recorded in the catalogue.</li>
<li><strong>Budget</strong>
Reported total project budget or funding amount, when available.</li>
<li><strong>Description</strong>
Short textual summary describing the project’s objectives, scope, and relevance to inland water systems and digital twin development.</li>
<li><strong>Resource</strong>
Reference link to an authoritative project source (e.g., project website, CORDIS entry).</li>
<li><strong>Inland water types</strong>
Categories of inland or transitional water systems explicitly addressed by the project (e.g., rivers, basins, reservoirs, urban water, coastal/transitional waters).
<strong>Typical use:</strong></li>
<li>Identifying implemented or pilot digital twin applications</li>
<li>Exploring methodologies, architectures, and tools used in practice</li>
<li>Tracing project outputs and outcomes</li>
</ul>
<hr>
<h5>Task_3.2.1_Scientific_Results</h5>
<p>This catalogue includes peer-reviewed scientific publications and research outputs relevant to inland water systems, hydrology, modelling, sensing, and digital twin concepts. Entries represent methodological, analytical, and theoretical contributions.</p>
<h6>Catalogue Structure – Scientific Results (Entry Fields)</h6>
<p>This catalogue is represented as a set of  <em>entries</em> , where each entry corresponds to a peer-reviewed scientific publication or research output relevant to inland water systems and digital twin development. Entries are derived from bibliographic and metadata fields and converted into structured text packs for use in NotebookLM. The fields are interpreted as follows:</p>
<ul>
<li><strong>ID</strong>
Unique identifier of the scientific result entry within the OpenKIWAS catalogue.</li>
<li><strong>Type</strong>
Classification of the research output (e.g., journal article, conference paper, review, technical report).</li>
<li><strong>Year</strong>
Year of publication or public release of the scientific result.</li>
<li><strong>Authors</strong>
List of authors as reported in the original publication metadata.</li>
<li><strong>Title</strong>
Title of the scientific publication or research output.</li>
<li><strong>Venue / Journal</strong>
Publication venue, such as the journal name, conference proceedings, or report series.</li>
<li><strong>DOI</strong>
Digital Object Identifier providing a persistent reference to the publication, when available.</li>
<li><strong>Research type</strong>
Characterisation of the research contribution (e.g., methodological, applied, theoretical, review, experimental).</li>
<li><strong>Water System</strong>
Type of water system or environment addressed by the research (e.g., rivers, basins, groundwater, reservoirs, urban water systems).</li>
<li><strong>Technical Focus</strong>
Main technical or scientific focus of the work (e.g., hydrological modelling, water quality analysis, flood forecasting, data assimilation, digital twin architectures).</li>
<li><strong>Abstract</strong>
Abstract or concise summary describing the objectives, methods, and key findings of the research.</li>
<li><strong>Link with Projects</strong>
Explicit references to research or innovation projects associated with the publication, if stated in the source metadata or publication.</li>
<li><strong>Link with Tools</strong>
Explicit references to software tools, models, platforms, or technologies used, developed, or evaluated in the research, when stated.</li>
<li><strong>Related policies</strong>
Policies, directives, or regulatory frameworks explicitly referenced or addressed by the research, if applicable.
<strong>Typical use:</strong></li>
<li>Identifying models, methods, and analytical approaches</li>
<li>Supporting evidence-based design decisions</li>
<li>Exploring validation and evaluation methods</li>
</ul>
<hr>
<h5>Task_3.2.2_Tools_and_Technologies</h5>
<p>This catalogue covers software tools, platforms, models, and technological solutions relevant to inland water digital twins. It includes simulation engines, modelling tools, data platforms, AI/ML technologies, and digital twin infrastructures.</p>
<h6>Catalogue Structure – Tools and Technologies (Entry Fields)</h6>
<p>This catalogue is represented as a set of  <em>entries</em> , where each entry corresponds to a software tool, platform, model, technology, or digital-twin–related solution relevant to inland water systems. Entries are derived from the original Excel metadata and converted into structured text packs for use in NotebookLM. The fields are interpreted as follows:</p>
<ul>
<li><strong>ID</strong>
Unique identifier of the tool/technology entry within the OpenKIWAS catalogue.</li>
<li><strong>Title</strong>
Name of the tool, platform, model, or technology as recorded in the catalogue.
<em>(Note: this corresponds to the original Excel column “Name”, which is normalised to “Title” in the text-pack export.)</em></li>
<li><strong>Technology</strong>
The main technology, method, or technological family associated with the entry (e.g., simulation engine, modelling framework, AI/ML method, data platform, digital twin component).</li>
<li><strong>Type</strong>
Classification of the entry (e.g., software tool, platform, library, model, service, digital twin framework), as captured in the catalogue.</li>
<li><strong>Data used as input</strong>
Description of the data sources or data types required or typically used by the tool/technology (e.g., sensor/SCADA data, hydrological time series, Earth Observation products, meteorological datasets).</li>
<li><strong>Produced datasets (openly available)</strong>
Datasets produced by the tool/technology that are reported as openly available, if any (e.g., outputs published as open datasets, derived products, benchmark datasets).</li>
<li><strong>Demo (video if available)</strong>
Reference to a demonstration resource (e.g., video demo, recorded presentation) where available.</li>
<li><strong>Paper (if available)</strong>
Reference to an associated scientific publication describing, evaluating, or introducing the tool/technology, when available.</li>
<li><strong>Paper DOI (if available)</strong>
DOI of the associated publication, when recorded.</li>
<li><strong>Project ID (if available)</strong>
Identifier of an associated project entry (if the tool/technology is explicitly linked to a project in the catalogue metadata).</li>
<li><strong>Project Acronym (if available)</strong>
Acronym of an associated project (if explicitly linked), supporting human-readable identification and cross-referencing.</li>
<li><strong>Service description</strong>
Short textual description of what the tool/technology provides (capabilities, functions, intended use), particularly in the context of inland water systems and digital twin applications.
<strong>Typical use:</strong></li>
<li>Identifying candidate tools and technologies</li>
<li>Comparing functional capabilities</li>
<li>Supporting architecture and pipeline design</li>
</ul>
<hr>
<h5>Task_3.3.1_Open_Data_and_Sensing_Sources_Copernicus_Data_Sources</h5>
<p>This catalogue focuses on Earth Observation data and services provided through the Copernicus programme. It includes satellite products, derived datasets, and services relevant to inland water monitoring, modelling, and analysis.</p>
<h6>Catalogue Structure – Copernicus Data Sources (Entry Fields)</h6>
<p>This catalogue is represented as a set of  <em>entries</em> , where each entry corresponds to an Earth Observation (EO) dataset, product, or service provided through the Copernicus programme and relevant to inland water monitoring, modelling, and digital twin workflows. Entries are derived from the original metadata fields and converted into structured text packs for use in NotebookLM. The fields are interpreted as follows:</p>
<ul>
<li><strong>ID</strong>
Unique identifier of the Copernicus data source entry within the OpenKIWAS catalogue.</li>
<li><strong>Source organisation</strong>
Organisation responsible for providing, maintaining, or distributing the dataset or service (e.g., Copernicus service provider, EO data hub, associated institution).</li>
<li><strong>Title</strong>
Name of the Copernicus dataset, product, or service as recorded in the catalogue.
<em>(Note: this corresponds to the original Excel column “Name”, which is normalised to “Title” in the text-pack export.)</em></li>
<li><strong>Type</strong>
Classification of the entry (e.g., satellite product, derived dataset, monitoring service, data access service), as recorded in the catalogue.</li>
<li><strong>Area</strong>
Thematic or application area covered by the dataset/service (e.g., land, water, climate, emergency management), as represented in the catalogue.</li>
<li><strong>Quality</strong>
Quality-related notes or attributes recorded for the dataset/service (e.g., validation status, stated uncertainty, quality flags), where available.</li>
<li><strong>Availability</strong>
Statement of accessibility (e.g., open access, registration required, restricted access), as captured in the entry.</li>
<li><strong>Update frequency</strong>
How often the dataset/service is updated or refreshed (e.g., near-real-time, daily, weekly, monthly, event-based), where available.</li>
<li><strong>Legal restrictions</strong>
Licensing constraints, access conditions, or usage restrictions associated with the dataset/service, as recorded.</li>
<li><strong>Description</strong>
Short textual summary describing what the dataset/service provides and why it is relevant for inland water applications.</li>
<li><strong>Variable(s)</strong>
Key variables provided by the dataset/service (e.g., water extent, surface temperature, precipitation proxies, land cover, relevant EO-derived indicators).</li>
<li><strong>Spatial coverage</strong>
Geographic coverage of the dataset/service (e.g., global, Europe-wide, regional, basin-specific), as recorded.</li>
<li><strong>Spatial resolution</strong>
Spatial granularity of the dataset (e.g., pixel size, grid resolution), where available.</li>
<li><strong>Temporal coverage</strong>
Time range covered by the dataset (e.g., historical period, start date to present), where available.</li>
<li><strong>Temporal resolution</strong>
Temporal granularity (e.g., hourly, daily, weekly composites, monthly aggregates), where available.</li>
<li><strong>Resource</strong>
Reference link to an authoritative dataset/service page or registry record.</li>
<li><strong>API Protocol</strong>
Access mechanism or interface used for programmatic retrieval (e.g., REST API, OPeNDAP, WMS/WFS, STAC, FTP), if provided.</li>
<li><strong>Limitations</strong>
Constraints noted in the catalogue entry (e.g., gaps, known uncertainties, spatial/temporal limits, access limitations, suitability constraints for specific use cases).
<strong>Typical use:</strong></li>
<li>Identifying EO data for digital twin inputs</li>
<li>Understanding spatial and temporal coverage</li>
<li>Supporting integration of EO data into modelling pipelines</li>
</ul>
<hr>
<h5>Task_3.3.1_Open_Data_and_Sensing_Sources_Zenodo_Data_Sources</h5>
<p>This catalogue contains open datasets hosted on Zenodo that are relevant to inland water systems. Datasets originate from research projects, experiments, and data-sharing initiatives and vary in structure and scope.</p>
<h6>Catalogue Structure – Zenodo Data Sources (Entry Fields)</h6>
<p>This catalogue is represented as a set of  <em>entries</em> , where each entry corresponds to an open dataset hosted on Zenodo and relevant to inland water systems, modelling, sensing, or digital twin applications. Entries are derived from Zenodo metadata and converted into structured text packs for use in NotebookLM. The fields are interpreted as follows:</p>
<ul>
<li><strong>ID</strong>
Unique identifier of the Zenodo dataset entry within the OpenKIWAS catalogue.</li>
<li><strong>Dataset Page</strong>
URL of the public Zenodo landing page for the dataset, providing human-readable access to metadata, files, and citation information.</li>
<li><strong>Metadata API URL</strong>
URL of the Zenodo API endpoint exposing the dataset metadata in machine-readable form, supporting programmatic access and integration.</li>
<li><strong>Title</strong>
Title of the dataset as published on Zenodo.</li>
<li><strong>DOI</strong>
Digital Object Identifier assigned to the dataset, providing a persistent and citable reference.</li>
<li><strong>Description</strong>
Textual description summarising the dataset contents, purpose, and relevance to inland water systems or related research.</li>
<li><strong>Creators</strong>
Individuals or organisations credited as creators of the dataset, as reported in the Zenodo metadata.</li>
<li><strong>Publication Date</strong>
Date on which the dataset version was published or made publicly available on Zenodo.</li>
<li><strong>Access</strong>
Access conditions for the dataset (e.g., open access, restricted access), as recorded in the metadata.</li>
<li><strong>Keywords</strong>
Keywords or tags associated with the dataset, used to describe thematic focus, methods, or application domains.</li>
<li><strong>License</strong>
Usage license under which the dataset is released (e.g., Creative Commons licenses), governing reuse and redistribution.</li>
<li><strong>Version</strong>
Version identifier of the dataset, when provided, supporting reproducibility and traceability across updates.
<strong>Typical use:</strong></li>
<li>Discovering reusable research datasets</li>
<li>Supplementing digital twin data inputs</li>
<li>Supporting reproducibility and validation</li>
</ul>
<hr>
<h5>Task_3.3.2_Catalogue_of_Data_and_Services_Hydrological_Data</h5>
<p>This catalogue includes hydrological data sources and services, such as river discharge, water levels, catchment characteristics, and hydrological measurements provided by national or international organisations.</p>
<h6>Catalogue Structure – Hydrological Data (Entry Fields)</h6>
<p>This catalogue is represented as a set of  <em>entries</em> , where each entry corresponds to a hydrological dataset or service relevant to inland water systems and digital twin applications. Entries are derived from original metadata fields and converted into structured text packs for use in NotebookLM. The fields are interpreted as follows:</p>
<ul>
<li><strong>ID</strong>
Unique identifier of the hydrological data/service entry within the OpenKIWAS catalogue.</li>
<li><strong>Source Organisation</strong>
Organisation responsible for producing, maintaining, or distributing the hydrological data or service (e.g., river basin authority, national hydrological service, research organisation).</li>
<li><strong>Title</strong>
Name of the hydrological dataset or service as recorded in the catalogue.
<em>(Note: this corresponds to the original Excel column “Name”, which is normalised to “Title” in the text-pack export.)</em></li>
<li><strong>Type</strong>
Classification of the data or service (e.g., observation dataset, monitoring service, modelled product, forecast service).</li>
<li><strong>Area</strong>
Thematic or application area associated with the data/service (e.g., hydrology, water resources, river monitoring), as captured in the catalogue.</li>
<li><strong>Quality</strong>
Quality-related attributes or notes recorded for the dataset/service (e.g., validation status, uncertainty information, quality flags), where available.</li>
<li><strong>Availability</strong>
Accessibility conditions of the data/service (e.g., open access, registration required, restricted), as recorded in the entry.</li>
<li><strong>Update frequency</strong>
How often the dataset or service is updated (e.g., near-real-time, daily, monthly, event-based), where specified.</li>
<li><strong>Legal restrictions</strong>
Licensing terms, access conditions, or usage restrictions associated with the data/service.</li>
<li><strong>Description</strong>
Short textual summary describing the dataset or service, its contents, and its relevance to inland water modelling, monitoring, and digital twin workflows.</li>
<li><strong>Variable(s)</strong>
Hydrological variables provided by the dataset/service (e.g., river discharge, water level, groundwater level, soil moisture, catchment characteristics), as recorded.</li>
<li><strong>Spatial coverage</strong>
Geographic extent of the dataset/service (e.g., basin-specific, national, regional, continental).</li>
<li><strong>Spatial resolution</strong>
Spatial granularity of the data (e.g., station spacing, grid resolution), where available.</li>
<li><strong>Temporal coverage</strong>
Time period covered by the dataset/service (e.g., historical records, start date to present).</li>
<li><strong>Temporal resolution</strong>
Temporal granularity of the data (e.g., sub-hourly, hourly, daily, monthly), where available.</li>
<li><strong>Resource</strong>
Reference link to an authoritative webpage or registry entry describing or providing access to the data/service.</li>
<li><strong>API Protocol</strong>
Programmatic access method or interface supported by the data/service (e.g., REST API, OPeNDAP, WMS/WFS, SOS), when available.
<strong>Typical use:</strong></li>
<li>Identifying core hydrological inputs</li>
<li>Supporting modelling, calibration, and validation</li>
<li>Understanding data availability and resolution</li>
</ul>
<hr>
<h5>Task_3.3.2_Catalogue_of_Data_and_Services_Meteorological_Data</h5>
<p>This catalogue focuses on meteorological data and services, including precipitation, temperature, wind, and related atmospheric variables relevant to inland water processes.</p>
<h6>Catalogue Structure – Meteorological Data (Entry Fields)</h6>
<p>This catalogue is represented as a set of  <em>entries</em> , where each entry corresponds to a meteorological dataset or service relevant to inland water processes and digital twin applications. Entries are derived from original metadata fields and converted into structured text packs for use in NotebookLM. The fields are interpreted as follows:</p>
<ul>
<li><strong>ID</strong>
Unique identifier of the meteorological data/service entry within the OpenKIWAS catalogue.</li>
<li><strong>Source Organisation</strong>
Organisation responsible for producing, maintaining, or distributing the meteorological data or service (e.g., national meteorological service, international organisation, research institute).</li>
<li><strong>Title</strong>
Name of the meteorological dataset or service as recorded in the catalogue.
<em>(Note: this corresponds to the original Excel column “Name”, which is normalised to “Title” in the text-pack export.)</em></li>
<li><strong>Type</strong>
Classification of the data or service (e.g., observation dataset, forecast product, reanalysis dataset, monitoring service).</li>
<li><strong>Area</strong>
Thematic or application area associated with the data/service (e.g., meteorology, climate, weather forecasting), as captured in the catalogue.</li>
<li><strong>Quality</strong>
Quality-related attributes or notes recorded for the dataset/service (e.g., validation status, uncertainty information, quality flags), where available.</li>
<li><strong>Availability</strong>
Accessibility conditions of the data/service (e.g., open access, registration required, restricted), as recorded in the entry.</li>
<li><strong>Update frequency</strong>
How often the dataset or service is updated (e.g., near-real-time, hourly, daily, seasonal), where specified.</li>
<li><strong>Legal restrictions</strong>
Licensing terms, access conditions, or usage restrictions associated with the data/service.</li>
<li><strong>Description</strong>
Short textual summary describing the dataset or service, its contents, and its relevance to inland water modelling and digital twin workflows.</li>
<li><strong>Variable(s)</strong>
Meteorological variables provided by the dataset/service (e.g., precipitation, temperature, wind, humidity, radiation), as recorded.</li>
<li><strong>Spatial coverage</strong>
Geographic extent of the dataset/service (e.g., global, continental, national, regional).</li>
<li><strong>Spatial resolution</strong>
Spatial granularity of the data (e.g., grid spacing, station density), where available.</li>
<li><strong>Temporal coverage</strong>
Time period covered by the dataset/service (e.g., historical range, start date to present).</li>
<li><strong>Temporal resolution</strong>
Temporal granularity of the data (e.g., hourly, daily, monthly), where available.</li>
<li><strong>Resource</strong>
Reference link to an authoritative webpage or registry entry describing or providing access to the data/service.</li>
<li><strong>API Protocol</strong>
Programmatic access method or interface supported by the data/service (e.g., REST API, OPeNDAP, WMS/WFS, FTP), when available.
<strong>Typical use:</strong></li>
<li>Identifying meteorological drivers for hydrological models</li>
<li>Supporting forecasting and scenario analysis</li>
<li>Integrating weather data into digital twin workflows</li>
</ul>
<hr>
<h5>Task_3.3.2_Catalogue_of_Data_and_Services_Oceanographic_Data</h5>
<p>This catalogue contains oceanographic data and services relevant to inland water–coastal interactions, such as sea level, tides, and marine conditions influencing river mouths, estuaries, and coastal systems.</p>
<h6>Catalogue Structure – Oceanographic Data (Entry Fields)</h6>
<p>This catalogue is represented as a set of  <em>entries</em> , where each entry corresponds to an oceanographic dataset or service relevant to inland water–coastal interactions and digital twin applications. Entries are derived from original metadata fields and converted into structured text packs for use in NotebookLM. The fields are interpreted as follows:</p>
<ul>
<li><strong>ID</strong>
Unique identifier of the oceanographic data/service entry within the OpenKIWAS catalogue.</li>
<li><strong>Source Organisation</strong>
Organisation responsible for producing, maintaining, or distributing the oceanographic data or service (e.g., marine research institute, national oceanographic service, international organisation).</li>
<li><strong>Title</strong>
Name of the oceanographic dataset or service as recorded in the catalogue.
<em>(Note: this corresponds to the original Excel column “Name”, which is normalised to “Title” in the text-pack export.)</em></li>
<li><strong>Type</strong>
Classification of the data or service (e.g., observation dataset, monitoring service, modelled product, forecast service).</li>
<li><strong>Area</strong>
Thematic or application area associated with the data/service (e.g., oceanography, coastal dynamics, marine environment), as captured in the catalogue.</li>
<li><strong>Quality</strong>
Quality-related attributes or notes recorded for the dataset/service (e.g., validation status, uncertainty information, quality flags), where available.</li>
<li><strong>Availability</strong>
Accessibility conditions of the data/service (e.g., open access, registration required, restricted), as recorded in the entry.</li>
<li><strong>Update frequency</strong>
How often the dataset or service is updated (e.g., near-real-time, hourly, daily, seasonal), where specified.</li>
<li><strong>Legal restrictions</strong>
Licensing terms, access conditions, or usage restrictions associated with the data/service.</li>
<li><strong>Description</strong>
Short textual summary describing the dataset or service, its contents, and its relevance to inland–coastal water modelling and digital twin workflows.</li>
<li><strong>Variable(s)</strong>
Oceanographic variables provided by the dataset/service (e.g., sea level, tides, currents, salinity, temperature, wave parameters), as recorded.</li>
<li><strong>Spatial coverage</strong>
Geographic extent of the dataset/service (e.g., global oceans, regional seas, coastal zones).</li>
<li><strong>Spatial resolution</strong>
Spatial granularity of the data (e.g., grid spacing, observation density), where available.</li>
<li><strong>Temporal coverage</strong>
Time period covered by the dataset/service (e.g., historical records, start date to present).</li>
<li><strong>Temporal resolution</strong>
Temporal granularity of the data (e.g., sub-hourly, hourly, daily), where available.</li>
<li><strong>Resource</strong>
Reference link to an authoritative webpage or registry entry describing or providing access to the data/service.</li>
<li><strong>API Protocol</strong>
Programmatic access method or interface supported by the data/service (e.g., REST API, OPeNDAP, WMS/WFS), when available.
<strong>Typical use:</strong></li>
<li>Supporting coupled inland–coastal digital twin applications</li>
<li>Defining boundary conditions for hydrodynamic models</li>
<li>Integrating oceanographic context into system analyses</li>
</ul>
<hr>
<h4>Final Note</h4>
<p>Each catalogue is intended to be used within its defined scope. Retrieval and reasoning in NotebookLM occur only within the active notebook context. Integration of findings across catalogues should be performed explicitly and transparently by the user.</p>
