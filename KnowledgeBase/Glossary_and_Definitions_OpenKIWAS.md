<h3>Glossary – OpenKIWAS Inland Water and Digital Twin Terminology</h3>
<h4>Glossary Introduction</h4>
<p>This glossary defines core terminology used across the OpenKIWAS knowledge base in the context of inland water systems, associated challenges, tools and technologies, and digital twin concepts. The terms reflect commonly used meanings in hydrology, water resources engineering, environmental monitoring, and digital twin research, while also clarifying their interpretation within OpenKIWAS.
The glossary is intended to support semantic consistency across catalogues and to reduce ambiguity when interpreting policies, projects, scientific results, tools, and data sources. Definitions are descriptive rather than prescriptive and do not imply endorsement of specific methods, technologies, or governance approaches.
Where terms may have multiple meanings across disciplines (e.g.  <em>digital twin</em> ,  <em>virtual water</em> ), the definition highlights the interpretation relevant to inland water digital twin applications, and notes alternative usages where necessary.</p>
<hr>
<h4>Inland Water Types</h4>
<h5>Water</h5>
<p><strong>Category:</strong>  Inland Water Type
<strong>Definition:</strong>
A general term for H₂O in any state (liquid, solid, vapour) and in any natural or engineered setting. In OpenKIWAS, “water” should be treated as an umbrella term that requires narrowing (e.g., river water, groundwater, coastal water).
<strong>Typical queries / subqueries:</strong></p>
<ul>
<li>Types of water systems</li>
<li>Classification of water bodies</li>
<li>Surface vs subsurface water</li>
</ul>
<hr>
<h5>River</h5>
<p><strong>Category:</strong>  Inland Water Type
<strong>Definition:</strong>
A flowing natural watercourse within a defined channel, typically draining a catchment/basin toward a lake, sea, or another river. Key traits: flow direction, discharge variability, connectivity, floodplain interaction.
<strong>Typical queries / subqueries:</strong></p>
<ul>
<li>River discharge</li>
<li>River flow modelling</li>
<li>Floodplain interaction</li>
</ul>
<hr>
<h5>Reservoir</h5>
<p><strong>Category:</strong>  Inland Water Type
<strong>Definition:</strong>
A managed, stored body of water—often created by a dam—used for water supply, irrigation, hydropower, flood control, or multi-purpose management. “Water reservoir” and “reservoir” are synonymous.
<strong>Typical queries / subqueries:</strong></p>
<ul>
<li>Reservoir operation</li>
<li>Storage management</li>
<li>Dam-controlled water bodies</li>
</ul>
<hr>
<h5>Lake</h5>
<p><strong>Category:</strong>  Inland Water Type
<strong>Definition:</strong>
A relatively still inland body of water surrounded by land. May be natural or artificial, fresh or saline, shallow or deep; interacts with inflows/outflows and groundwater.
<strong>Typical queries / subqueries:</strong></p>
<ul>
<li>Lake water balance</li>
<li>Lake level monitoring</li>
<li>Lake–groundwater interaction</li>
</ul>
<hr>
<h5>Snow</h5>
<p><strong>Category:</strong>  Inland Water Type
<strong>Definition:</strong>
Frozen precipitation accumulated on land. Hydrologically important as seasonal storage; melt contributes to runoff, river discharge, and groundwater recharge.
<strong>Typical queries / subqueries:</strong></p>
<ul>
<li>Snowmelt contribution</li>
<li>Seasonal water storage</li>
<li>Cryospheric inputs</li>
</ul>
<hr>
<h5>Groundwater</h5>
<p><strong>Category:</strong>  Inland Water Type
<strong>Definition:</strong>
Water stored below the Earth’s surface within saturated geological formations (aquifers). Moves slowly through pores/fractures; critical for baseflow to rivers and drought resilience.
<strong>Typical queries / subqueries:</strong></p>
<ul>
<li>Aquifer storage</li>
<li>Groundwater recharge</li>
<li>Baseflow contribution</li>
</ul>
<hr>
<h5>Wetlands</h5>
<p><strong>Category:</strong>  Inland Water Type
<strong>Definition:</strong>
Areas where water saturates soil or covers land for long periods (e.g., marshes, swamps, peatlands). Important for biodiversity, nutrient cycling, flood attenuation, and water quality regulation.
<strong>Typical queries / subqueries:</strong></p>
<ul>
<li>Wetland ecosystem services</li>
<li>Flood attenuation</li>
<li>Nutrient cycling</li>
</ul>
<hr>
<h5>Snow and Ice</h5>
<p><strong>Category:</strong>  Inland Water Type
<strong>Definition:</strong>
Combined cryospheric storage that includes seasonal snowpack and more persistent ice forms. Used when frozen-water components are treated together for water balance and climate sensitivity.
<strong>Typical queries / subqueries:</strong></p>
<ul>
<li>Cryosphere water storage</li>
<li>Climate-sensitive water balance</li>
</ul>
<hr>
<h5>Coastal Water</h5>
<p><strong>Category:</strong>  Inland Water Type
<strong>Definition:</strong>
Water in coastal zones influenced by both terrestrial runoff and marine processes (tides, salinity intrusion, storm surge). Relevant where inland waters meet the sea.
<strong>Typical queries / subqueries:</strong></p>
<ul>
<li>Salinity intrusion</li>
<li>River–sea interaction</li>
<li>Coastal boundary conditions</li>
</ul>
<hr>
<h5>Urban Water</h5>
<p><strong>Category:</strong>  Inland Water Type
<strong>Definition:</strong>
Water systems and flows in urban environments, including potable supply, stormwater drainage, wastewater collection and treatment, and interactions with impervious surfaces.
<strong>Typical queries / subqueries:</strong></p>
<ul>
<li>Urban water systems</li>
<li>Stormwater management</li>
<li>Wastewater networks</li>
</ul>
<hr>
<h5>Basin (Catchment / Watershed)</h5>
<p><strong>Category:</strong>  Inland Water Type
<strong>Definition:</strong>
A land area where precipitation drains to a common outlet (river point, lake, estuary). Fundamental unit for hydrological modelling, water accounting, and governance.
<strong>Typical queries / subqueries:</strong></p>
<ul>
<li>Basin-scale modelling</li>
<li>Catchment hydrology</li>
<li>Watershed management</li>
</ul>
<hr>
<h5>Rainfall (Precipitation)</h5>
<p><strong>Category:</strong>  Inland Water Type
<strong>Definition:</strong>
Water falling from the atmosphere. A primary driver for runoff, flooding, infiltration, and water availability; characterised by intensity, duration, frequency, and spatial distribution.
<strong>Typical queries / subqueries:</strong></p>
<ul>
<li>Rainfall intensity</li>
<li>Precipitation patterns</li>
<li>Runoff generation</li>
</ul>
<hr>
<h5>Ice</h5>
<p><strong>Category:</strong>  Inland Water Type
<strong>Definition:</strong>
Frozen water in forms such as river ice, lake ice, or ice cover. Affects flow resistance, flood risk (ice jams), and energy balance.
<strong>Typical queries / subqueries:</strong></p>
<ul>
<li>Ice jams</li>
<li>River ice dynamics</li>
</ul>
<hr>
<h5>Glacier</h5>
<p><strong>Category:</strong>  Inland Water Type
<strong>Definition:</strong>
A persistent mass of ice formed from compacted snow, moving under its own weight. Important as long-term water storage; meltwater affects seasonal flows and long-term water security.
<strong>Typical queries / subqueries:</strong></p>
<ul>
<li>Glacier melt contribution</li>
<li>Long-term water storage</li>
</ul>
<hr>
<h5>Lagoon</h5>
<p><strong>Category:</strong>  Inland Water Type
<strong>Definition:</strong>
A shallow water body separated from a larger water body by barriers such as sandbars or reefs. Can be saline or brackish; relevant for coastal–inland interactions and water quality dynamics.
<strong>Typical queries / subqueries:</strong></p>
<ul>
<li>Lagoon salinity</li>
<li>Coastal lagoons</li>
</ul>
<hr>
<h5>Transitional Water</h5>
<p><strong>Category:</strong>  Inland Water Type
<strong>Definition:</strong>
Water bodies at the interface between rivers and coastal waters (e.g., estuaries) with mixed fresh and saline influence; dynamic and sensitive to tides, discharge, and pollution.
<strong>Typical queries / subqueries:</strong></p>
<ul>
<li>Estuarine systems</li>
<li>Freshwater–saline mixing</li>
</ul>
<hr>
<h4>Refined / Composite Digital Twin Terminologies</h4>
<h5>Digital Twin</h5>
<p><strong>Category:</strong>  Digital Twin Terminology
<strong>Definition:</strong>
A virtual representation of a physical system that stays connected to it through data (often near-real-time), enabling monitoring, simulation, prediction, and decision support. Key feature: an operational feedback loop, not just an offline model.
<strong>Typical queries / subqueries:</strong></p>
<ul>
<li>Definition of digital twin</li>
<li>Difference between digital twin and model</li>
<li>Operational digital twin</li>
</ul>
<hr>
<h5>Water Modeling</h5>
<p><strong>Category:</strong>  Digital Twin Terminology
<strong>Definition:</strong>
The practice of building and using hydrological/hydraulic/water-quality models for understanding and forecasting water behaviour. Can be a component of a digital twin, but does not necessarily imply real-time coupling.
<strong>Typical queries / subqueries:</strong></p>
<ul>
<li>Hydrological modelling</li>
<li>Hydraulic models</li>
<li>Water quality models</li>
</ul>
<hr>
<h5>Digital Twin Water / Water Digital Twin</h5>
<p><strong>Category:</strong>  Digital Twin Terminology
<strong>Definition:</strong>
A digital twin applied to water systems (rivers, reservoirs, networks, treatment plants, basins). Typically integrates sensor/EO data, models, and operational context to support decisions.
<strong>Typical queries / subqueries:</strong></p>
<ul>
<li>Water digital twin examples</li>
<li>Digital twin for rivers or reservoirs</li>
<li>Integrated water twin</li>
</ul>
<hr>
<h5>Water Systems</h5>
<p><strong>Category:</strong>  Digital Twin Terminology
<strong>Definition:</strong>
A broad term covering natural water bodies/processes and engineered infrastructure (supply, distribution, wastewater, stormwater). Often treated as a coupled socio-technical system.
<strong>Typical queries / subqueries:</strong></p>
<ul>
<li>Definition of water systems</li>
<li>Natural and engineered water systems</li>
<li>Socio-technical water systems</li>
</ul>
<hr>
<h5>Hydrological Twin / Hydrology Twin</h5>
<p><strong>Category:</strong>  Digital Twin Terminology
<strong>Definition:</strong>
A digital twin focused on hydrological processes (rainfall–runoff, soil moisture, groundwater, basin-scale water balance). Often basin-centric, with emphasis on forecasting and extremes.
<strong>Typical queries / subqueries:</strong></p>
<ul>
<li>Hydrological digital twin</li>
<li>Basin-scale digital twin</li>
<li>Rainfall–runoff twin</li>
</ul>
<hr>
<h5>Water Monitoring</h5>
<p><strong>Category:</strong>  Digital Twin Terminology
<strong>Definition:</strong>
Data acquisition and observation practices for water quantity and quality, across in-situ sensors, sampling, SCADA, and EO. In a twin context, monitoring provides the live data feed.
<strong>Typical queries / subqueries:</strong></p>
<ul>
<li>Water monitoring systems</li>
<li>Sensors for water monitoring</li>
<li>Real-time water data</li>
</ul>
<hr>
<h5>Inland Analytics</h5>
<p><strong>Category:</strong>  Digital Twin Terminology
<strong>Definition:</strong>
Data-driven analysis focused on inland waters: trend detection, anomaly detection, forecasting, classification (e.g., water extent), and decision analytics across basins and inland water assets.
<strong>Typical queries / subqueries:</strong></p>
<ul>
<li>Inland water analytics</li>
<li>Data analytics for rivers</li>
<li>Water-related decision analytics</li>
</ul>
<hr>
<h5>Smart Water</h5>
<p><strong>Category:</strong>  Digital Twin Terminology
<strong>Definition:</strong>
An operational paradigm using sensors, communications, analytics, and automation to improve water infrastructure performance (efficiency, quality, resilience).
<strong>Typical queries / subqueries:</strong></p>
<ul>
<li>Smart water systems</li>
<li>Digitalisation of water utilities</li>
<li>Automated water networks</li>
</ul>
<hr>
<h5>Water Simulation</h5>
<p><strong>Category:</strong>  Digital Twin Terminology
<strong>Definition:</strong>
Computational simulation of water system dynamics (hydraulic transients, flood propagation, reservoir operations, contaminant transport).
<strong>Typical queries / subqueries:</strong></p>
<ul>
<li>Water system simulation</li>
<li>Flood simulation</li>
<li>Scenario analysis in water systems</li>
</ul>
<hr>
<h5>Water Quality Twin</h5>
<p><strong>Category:</strong>  Digital Twin Terminology
<strong>Definition:</strong>
A digital twin specialised in water quality dynamics (chemical/biological parameters), including monitoring, modelling, early warning, and intervention planning.
<strong>Typical queries / subqueries:</strong></p>
<ul>
<li>Water quality digital twin</li>
<li>Contamination monitoring</li>
<li>Algal bloom prediction</li>
</ul>
<hr>
<h5>Virtual Water</h5>
<p><strong>Category:</strong>  Digital Twin Terminology
<strong>Definition:</strong>
(1) In modelling: the “virtualised” representation of water processes (digital models/replicas).
(2) In policy/economics: “virtual water trade” (water embedded in products).
<strong>Typical queries / subqueries:</strong></p>
<ul>
<li>Virtual water modelling</li>
<li>Virtual water trade</li>
<li>Embedded</li>
</ul>
