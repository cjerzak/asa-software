
---

## Implementation status (applied to `asa-paper/Paper/ms.tex`)

- [x] Phase 0: tighten Intro contract + reproducibility target (DONE)
- [x] Phase 1: add formal auditability/comparability subsection (DONE)
- [x] Phase 1: add abstention as selective measurement subsection (DONE)
- [x] Phase 2: specify evidence hierarchy + tiered sufficiency rule (E) (DONE)
- [x] Phase 2: make conflict policy explicit (DONE)
- [x] Phase 2: add ordered date-recovery heuristics for \(d(s)\) (DONE)
- [x] Phase 2: add entity disambiguation rule (DONE)
- [x] Phase 2: add trace schema + auditor checklist (DONE)
- [x] Phase 3: expand Data section (leader records, codebooks, linkage) (DONE)
- [x] Phase 4: expand Related Work (measurement validity, transparency, abstention) (DONE)
- [x] Phase 5: add threats-to-validity subsection + explicit metric formulas (DONE)
- [x] Phase 6: add JOP-style replication package paragraph + web persistence cites (DONE)
- [x] Phase 7: expand Limitations/Ethics (coverage bias, link rot, sensitive traits) (DONE)
- [x] Add missing bib entries with verified metadata (DONE)
- [ ] Quick win: add 1-page ASA specification table (TODO)
- [ ] Quick win: add worked audit example with a real record id (TODO)

## 1) What currently reads as “incomplete” to a JOP reviewer

### A. The “formalism” is narrowly focused on temporal leakage

You have a strong DAG for temporal leakage, but *auditability* and *cross-national comparability* are currently described as hazards, not formal properties with operational definitions. A JOP reviewer will want:

* **What does “auditable” mean formally?** What is the object being audited (claim-level evidence vs. raw action logs)?
* **What is the reproducibility target?** “Re-running on the contemporary web won’t match” is true, but you need a crisp definition of what *will* match (recomputing from a frozen trace store).
* **What makes it cross-nationally comparable?** “Closed codebooks” is right; you should explicitly formalize the mapping function and the no-new-classes guarantee.

### B. The methodology section is “protocol-shaped” but under-specified

ASA is presented as a protocol, but key elements are referenced as configuration objects without being spelled out:

* evidence sufficiency rule (E) (what counts as “high confidence”?)
* the **source hierarchy** and **independence** criteria
* conflict resolution vs abstention rules
* the mechanics of date recovery (d(s)) (what methods, what failure modes?)
* disambiguation logic (homonyms; multiple politicians with the same name; transliterations)
* the trace store schema (what fields are stored; how an auditor replays a decision)

Without those, a reviewer may say: “This is a good idea but not yet a fully specified measurement instrument.”

### C. The data section is too thin for political science norms

Right now, a reader can’t tell:

* where leader–records come from (Archigos? V-Dem? bespoke?)
* how leader–records were matched to expert party labels (entity resolution is nontrivial)
* how codebooks are constructed and versioned
* what “expert labels” means (single coder? multiple? adjudication? timing?)

This is the single biggest “completeness” gap because JOP reviewers are often dataset- and measurement-attentive.

### D. Replication expectations need to be stated concretely in *JOP language*

JOP has specific replication norms (page limits for submission; replication package expectations; Dataverse upload). Even if UChicago pages are hard to fetch directly, guidance summarizes that replication materials should include a README, datasets, codebook, and code, and be uploaded as a single zip to JOP’s Dataverse. ([csdp.princeton.edu][1])
Right now your transparency section is philosophically aligned, but it needs a “here is exactly what the replication package will contain” paragraph.

---

## 2) High-impact text you can paste in (with insertion points)

I’m giving you **copy-paste LaTeX blocks**. They are written to (a) not require new packages, and (b) integrate into your current structure.

### 2.1 Add formalism for auditability + comparability (Section 2, after temporal leakage formalization)

**Insertion point:** end of Section 2.1 / after Table 1, or as a new subsection right after \ref{sec:temporal-formal}.

```latex
\subsection{Auditability and comparability as formal properties\label{sec:formal-audit-compare}}

Temporal leakage is only one way search-enabled coding can fail as measurement.
To make retrieval an admissible instrument for large-$N$ political data construction, we also require
(1) \emph{auditability} (a third party can reconstruct why a label was emitted) and
(2) \emph{comparability} (outputs remain in a stable, closed set of classes across country-years).

\paragraph{The trace as the measurement object.}
Let $\Omega_t$ denote the (unobserved) state of the web at time $t$.
A tool-using coding episode for record $i$ produces a \emph{trace}
\[
  T_i = \big( (a_{i1}, o_{i1}, \kappa_{i1}), \ldots, (a_{iK}, o_{iK}, \kappa_{iK}) \big),
\]
where each step records the agent action $a_{ik}$ (e.g., a query or page fetch),
the observed tool output $o_{ik}$ (e.g., ranked results and snippets), and a timestamp $\kappa_{ik}$.
The reported label is a function of the trace and task specification:
\[
  \hat{X}_{it} = M(T_i;\,\mathcal{C}_i,\tau_i,E,\theta),
\]
where $\mathcal{C}_i$ is the closed codebook, $\tau_i$ is the temporal cut,
$E$ is an evidence sufficiency rule, and $\theta$ denotes implementation parameters
(e.g., mapping thresholds, source-type rules).

\paragraph{Auditability.}
We say a coding procedure is \emph{auditably reproducible} for a reported result if,
given a \emph{frozen} trace store $\mathcal{T}=\{T_i\}_{i=1}^N$ and a versioned task specification
$(\mathcal{C}_i,\tau_i,E,\theta)$, an independent auditor can deterministically recompute every
reported label and every aggregate statistic in the paper by replaying $M(\cdot)$ on $\mathcal{T}$.
This is the relevant reproducibility target for web-based measurement: rerunning retrieval on the live web
is not expected to match, but recomputation from an archived trace store is.
Recent work in agentic systems makes a related point: raw action logs are often insufficient for
claim-level auditability, motivating structured evidence encodings that preserve claim--evidence relations
rather than only tool calls \citep{rasheed2026fluent}.

\paragraph{Comparability via closed-world mapping.}
Cross-national comparability requires that recorded party labels be stable and enumerable.
Formally, ASA enforces a closed-world constraint by requiring $\hat{X}_{it}\in\mathcal{C}_i$ or abstention.
Let $g(\cdot)$ map extracted surface strings from evidence into codebook entries.
Closed-world comparability requires $g$ to be \emph{non-expansive}:
it may normalize benign surface variation (punctuation, spacing, acronyms) but must never introduce
a class outside $\mathcal{C}_i$. When $g$ is undefined or evidence supports multiple codebook entries,
ASA abstains rather than generating a novel label.
This operationalizes a basic measurement-validity principle: we prefer missingness that is explicit and auditable
to uncontrolled label drift that silently changes the meaning of a variable across units and time
\citep{adcock2001measurement,munck2002conceptualizing,lazer2014parable}.
```

**What this accomplishes:**

* turns “auditability” into a reproducibility target (recompute from frozen traces)
* turns “comparability” into a non-expansive mapping property
* links to measurement validity classics + the “big data trap” warning (Google Flu)

You’ll need bib entries for `adcock2001measurement`, `munck2002conceptualizing`, `lazer2014parable`, and you already have `rasheed2026fluent` cited in your draft; it exists as an arXiv paper (Feb 2026) on claim-level auditability. ([arXiv][2])

---

### 2.2 Expand “abstention” into a formal selective-measurement framework (Methods, after Protocol commitments)

**Insertion point:** Section 4.1 (Protocol commitments) or right after Protocol 1.

```latex
\subsection{Abstention as selective measurement\label{sec:selective-measurement}}

ASA’s abstention rule is not merely an implementation detail; it defines the statistical object that
is being produced. Let $Y_{it}$ be the expert-coded party label (when available).
ASA returns a \emph{selective} prediction $\hat{Y}_{it}\in \mathcal{C}_i \cup \{\bot\}$,
where $\bot$ denotes abstention.
Two quantities therefore matter jointly:
\[
  \text{Coverage} \;\gamma \equiv \Pr(\hat{Y}_{it}\neq \bot), 
  \qquad
  \text{Conditional error} \; R \equiv \Pr(\hat{Y}_{it}\neq Y_{it}\mid \hat{Y}_{it}\neq \bot).
\]
A key design goal is to reduce $R$ by tightening evidence requirements, accepting the mechanical consequence
that $\gamma$ falls.
This is the classic error--reject tradeoff in recognition systems \citep{chow1970reject},
and closely related to modern work on selective classification that treats abstention as a principled way
to control risk by withholding low-support predictions \citep{geifman2017selective}.
In elite measurement settings, abstention is attractive because the withheld cases can be routed to human coders
or left missing under transparent, replayable rules---a preferable failure mode to un-auditable guesswork.
```

**What this accomplishes:**

* gives a clean formal definition of what your accuracy numbers mean
* frames abstention as principled, not ad hoc
* sets up reviewers to accept the “coverage vs precision” logic

---

### 2.3 Add an explicit evidence hierarchy + sufficiency rule (E) (Methods, currently underspecified)

**Insertion point:** in Section 4.2 Task formalization (right after you define (E) in Protocol 1), or as a new subsection “Evidence sufficiency and confidence tiers.”

```latex
\subsection{Evidence sufficiency and confidence tiers\label{sec:evidence-rule}}

The protocol parameter $E$ encodes what counts as \emph{sufficient evidence} for emitting a high-confidence label.
In party-affiliation coding, we implement $E$ as an evidence-tier rule that privileges primary, contemporaneous sources
and requires independence across sources.

\paragraph{Source types.}
Each retrieved source $s$ is assigned a source-type tag $\mathrm{type}(s)$ using a lightweight heuristic classifier
(domain lists + page cues). We distinguish:
(i) \emph{primary/official} (e.g., government, parliament, election commission, party roster pages),
(ii) \emph{reputable secondary} (major news outlets, established reference works), and
(iii) \emph{tertiary/aggregators} (Wikipedia leads, scraped bios, low-editability directories).
Tertiary sources may be used for navigation (query expansion) but are not, by themselves, sufficient for a high-evidence decision.

\paragraph{Independence.}
To reduce correlated error, we treat two sources as independent if they come from different base domains
and are not obvious mirrors or syndicated copies. This criterion is deliberately coarse but easy to audit in traces.

\paragraph{Tiered sufficiency rule.}
Let $\mathcal{S}^{\mathrm{adm}}_{it}(\tau_i)$ be the admissible (dated, pre-$\tau_i$) evidence set.
ASA assigns a record to one of three tiers:

\begin{enumerate}[leftmargin=*, itemsep=2pt]
\item \textbf{Tier A (high evidence):} At least two independent admissible sources map to the same codebook label,
including at least one primary/official source with recoverable date.
\item \textbf{Tier B (moderate evidence):} Either (i) one admissible primary/official source maps cleanly to a label,
or (ii) two independent reputable secondary sources agree, but primary evidence is unavailable.
\item \textbf{Tier C (weak/undated/conflicting):} Evidence is undated, post-period, maps to multiple labels,
or relies only on tertiary sources.
\end{enumerate}

We emit a high-confidence label only under Tier A (and, optionally, Tier B under task-specific conditions stated in the appendix).
All Tier C cases are abstentions. This makes the abstention boundary explicit and replayable:
a downstream user can tighten or relax $E$ and recompute all headline results from the same frozen trace store.
```

**Why reviewers like this:** it turns “high confidence” into an auditable rule rather than a model-internal belief.

---

### 2.4 Add a trace schema + “auditor checklist” (Methods or Transparency)

**Insertion point:** either Section 4.5 (Provenance) or Section 9 (Transparency). This is *very* JOP-friendly because it reads like replication infrastructure.

```latex
\subsection{Trace schema and audit procedure\label{sec:trace-schema}}

A key claim of ASA is that it produces measurements that are \emph{auditable by construction}.
To make this concrete, the trace store persists two linked objects per record $i$:

\paragraph{(i) A structured decision record.}
This is the minimal unit needed to reproduce the paper’s tables:
\[
  D_i = \{\text{record id},\; \hat{Y}_{it},\; \text{tier},\; \mathcal{C}_i,\; \tau_i,\;
  \text{supporting citations},\; \text{conflict flags},\; \text{software version}\}.
\]
The “supporting citations” field points to specific snippet spans within retrieved pages (not only URLs),
so an auditor can verify that the cited text actually supports the mapped codebook label.

\paragraph{(ii) A full interaction trace.}
For each tool call $k$, we store: query string or URL, ranked results, snippet text (bounded length),
timestamp, domain, recovered publication/update date (if any), and extraction metadata.
Storing bounded snippets rather than full pages reduces privacy and copyright exposure while preserving enough context for audit.

\paragraph{Audit checklist (replayable).}
Given $(D_i, T_i)$, an auditor can verify a high-confidence label by checking:
(1) all cited sources are admissible under the temporal cut ($d(s)\le\tau_i$),
(2) cited spans contain an explicit party mention that maps non-expansively into $\mathcal{C}_i$,
(3) the required number of independent sources is met for the claimed tier,
and (4) there is no unresolved conflict with similarly supported alternatives.
This moves verification from “trust the model” to “inspect the stored evidence.”
```

Optionally add a small table of required fields (record id, query, timestamp, URL, snippet hash, extracted date, etc.). You already have `tabularx`, so you can do that without new packages.

---

### 2.5 Greatly expand the Data section (this is the biggest completeness win)

**Insertion point:** Section 5, immediately after 5.1 or replacing 5.1–5.3 with a fuller narrative.

Because I don’t know your exact leader dataset + expert party labels source, I’m giving you **a fill-in template** that reads “complete” but doesn’t invent facts. Replace bracketed placeholders.

```latex
\subsection{Leader records, temporal scope, and unit construction\label{sec:leader-records}}

We begin from an existing cross-national leader panel [\emph{DATA SOURCE CITATION HERE}],
which records [chief executives / heads of government / relevant elite offices] by country and year.
From this source we construct leader--records indexed by $\langle$person, country, year$t\rangle$.
When multiple leaders serve within a year, we follow a pre-specified rule:
[retain the leader serving on a reference date; or create multiple records per year with tenure dates; or code majority-of-year].
This rule is necessary because party affiliation may change within a calendar year and because sources often summarize careers retrospectively.

We normalize person identifiers using [name canonicalization + diacritics handling + transliteration rules],
and we retain auxiliary disambiguators when available (e.g., office title, birth year, tenure dates).
These fields are stored in the trace store so that auditors can detect homonyms and verify entity resolution decisions.

\subsection{Expert labels and codebook construction\label{sec:codebooks}}

For each country-year, domain experts provide a closed codebook $\mathcal{C}_i$ of admissible party labels.
Operationally, codebooks are derived from [expert-coded party datasets / official election records / curated party lists],
then versioned and frozen for the ASA run.
When party systems change through mergers, rebrandings, or coalitions, the codebook records both the canonical label
and a list of aliases used for conservative normalization.

Expert party labels used for evaluation come from [\emph{EXPERT SOURCE CITATION HERE}].
We treat these labels as the gold standard for validation, but we emphasize that the ASA trace store preserves enough evidence
to support audits and disagreement adjudication, consistent with measurement best practices
\citep{adcock2001measurement,munck2002conceptualizing}.

\subsection{Record linkage and match quality\label{sec:record-linkage}}

Because leader panels and expert-coded party sources often use non-identical naming conventions,
we perform record linkage to identify overlapping leader--records.
We implement a conservative matching procedure using [exact matches on normalized names + auxiliary identifiers],
then route ambiguous matches to manual review.
This conservative linkage step is important: linkage error can otherwise be misattributed to the coding agent.
We report match rates by country and period in Appendix~[X] and preserve linkage decisions in the trace store so they are auditable.
```

**Citations to add here (almost certainly needed):** a leader dataset (Archigos is common) and a party dataset (DPI, Party Facts, V-Party, ParlGov, etc. depending on your data). Examples are below in the “Missing citations” list.

---

### 2.6 Add a “Threats to validity” paragraph for the evaluation design (Validation section)

**Insertion point:** end of 6.1–6.3, before Results.

```latex
\subsection{Threats to validity in evaluating web-based measurement\label{sec:eval-threats}}

Two evaluation design choices are essential to interpret ASA’s headline numbers.

First, accuracy is reported \emph{conditional on non-abstention}.
This is appropriate for a selective measurement instrument, but it means that evaluation is performed on the subset of records
that satisfy the evidence sufficiency rule $E$ under the temporal cut.
As a result, accuracy alone is not meaningful without the corresponding coverage $\gamma$ and a description of which cases are withheld
(e.g., early years with sparse web records, low-salience elites, or non-English contexts).

Second, web retrieval creates a potential dependence between the information environment available to experts and to ASA.
To avoid conflating “shared sources” with methodological success, ASA stores its full evidence trails:
a reader can directly inspect whether correct labels are supported by contemporaneous primary sources or by retrospective summaries.
This is one reason temporal governance is a core part of the protocol rather than a post-hoc evaluation filter.
```

This paragraph “answers the reviewer in advance,” without requiring new results.

---

### 2.7 Expand the replication paragraph in the JOP “language” (Transparency section)

JOP’s submission length and replication expectations should be acknowledged explicitly. Available summaries note that research articles are limited to ~35 pages at initial submission and that replication materials should include (at least) a README, dataset(s), a codebook, and code—uploaded as a single zip to JOP’s Dataverse. ([Chicago Journals][3])

**Insertion point:** Section 9 (Transparency), after your current first paragraph.

```latex
\paragraph{Replication package contents (JOP).}
In line with JOP reproducibility expectations, the replication package for this article will include:
(i) a \texttt{README} specifying software versions and execution order,
(ii) the analysis dataset(s) used to generate every table and figure in the main text and appendix,
(iii) a codebook describing every variable used, and
(iv) scripts that reproduce the paper from a frozen trace store snapshot
(i.e., recompute all accuracy/coverage statistics and regenerate all figures without re-querying the live web).
Where full trace release is constrained (e.g., terms of service, privacy, or copyright),
we will provide de-identified example traces, the complete schema, and an access pathway for qualified auditors
to verify claims against the full trace store under controlled conditions.
```

---

## 3) Missing citations/sources (what to add, and where)

Below are *high-yield* citations that will make the manuscript feel grounded to JOP reviewers. I’m grouping them by what “hole” they fill.

### A. Political science measurement validity (anchors your “retrieval as measurement” framing)

Add these early (hazards or formalism), and again when you defend abstention and codebooks:

* **Adcock & Collier (2001)**, “Measurement Validity: A Shared Standard for Qualitative and Quantitative Research.”
* **Munck & Verkuilen (2002)**, “Conceptualizing and Measuring Democracy: Evaluating Alternative Indices.” ([SAGE Journals][4])
* (Optional but nice) **Coppedge et al. (2011)** “Conceptualizing and Measuring Democracy: A New Approach” (Perspectives on Politics) if you want a democracy-measurement tie-in. ([UCG - Univerzitet Crne Gore][5])

**Where:** Section 2 (hazards), new formalism subsection, and Data section when explaining codebooks/expert labels.

### B. Digital trace pitfalls / “big data measurement is not plug-and-play”

* **Lazer et al. (2014)** “The Parable of Google Flu: Traps in Big Data Analysis.” ([PubMed][6])
  This is a canonical political-methods-friendly reference for “dynamic, shifting data generating processes” and measurement error in digital traces.

**Where:** Section 2 (hazards), and Limitations.

### C. Transparency & research practices in political science (speaks directly to JOP norms)

* **DA-RT joint statement** (originally 2014/2015; Cambridge hosts it as an EJPR item). ([Cambridge University Press & Assessment][7])
* **Moravcsik (2014)** on “active citation” / qualitative transparency. ([Princeton University][8])
* **ATI (Annotation for Transparent Inquiry)** materials (QDR / Moravcsik lineage). ([qdr.syr.edu][9])
* **Gentzkow & Shapiro (2014)** “Code and Data for the Social Sciences: A Practitioner’s Guide.” ([Stanford University][10])

**Where:** Related work + Transparency section.

### D. Abstention / selective prediction as a principled design

You should cite at least one “reject option” classic and one modern selective classification paper:

* **Chow (1970)** “On Optimum Recognition Error and Reject Tradeoff.” ([ACM Digital Library][11])
* **Geifman & El-Yaniv (2017)** “Selective Classification for Deep Neural Networks.” ([arXiv][12])
  Optional: **Angelopoulos & Bates (2021)** conformal prediction tutorial if you want to connect to coverage guarantees. ([arXiv][13])

**Where:** Methods (abstention subsection) + Metrics.

### E. Web persistence, link rot, and “as-of” retrieval (supports your temporal governance + trace store claims)

You *need* something here because reviewers will worry about link rot and drifting pages.

* **Klein et al. (2014)** “Scholarly Context Not Found: One in Five Articles Suffers from Reference Rot” (PLOS ONE). ([PLOS][14])
* **Memento** time-based access: RFC 7089 “HTTP Framework for Time-Based Access to Resource States.” ([IETF Datatracker][15])
* (Optional) **Perma.cc** / legal scholarship on robust links (Zittrain et al. 2014). ([Harvard Law Review][16])

**Where:** Temporal governance subsection + Transparency section (when discussing snapshots/hashes).

### F. Entity resolution / record linkage (if you do any matching across datasets—which you do)

Even a single paragraph and 1–2 citations makes the paper feel much more “methodologically complete.”

* **Fellegi & Sunter (1969)** “A Theory for Record Linkage.” ([Cornell Computer Science][17])

**Where:** Data section (“Record linkage and match quality”).

### G. Core political datasets you are implicitly adjacent to (reviewers will expect these names)

Add citations for whichever you actually use (do not cite all if you don’t use them):

* **Archigos**: Goemans, Gleditsch & Chiozza (2009) “Introducing Archigos.” ([SAGE Journals][18])
* **DPI**: Beck et al. (2001) “Database of Political Institutions.” ([OUP Academic][19])
* **Party Facts**: Döring & Regel (2019). ([SAGE Journals][20])
* **ParlGov**: Döring & Manow (data infrastructure; cite the relevant ParlGov paper or project description). ([ParlGov][21])
* **V-Party / V-Dem party identity datasets** if relevant. ([V-Dem][22])

**Where:** Data section + Related work + “why party affiliation matters.”

### H. “LLMs in political science” positioning (optional but useful for JOP framing)

* A review paper that legitimizes the topic: “Large language models and political science” (Frontiers, 2023). ([Frontiers][23])

**Where:** Related work and/or Intro.

### I. Agent auditability / structured traces (you already cite Rasheed; keep it, but contextualize)

Your cited `rasheed2026fluent` corresponds to an arXiv paper (Feb 2026) on claim-level auditability for deep research agents. ([arXiv][2])
That’s *excellent* support for your “structured traces > raw logs” claim—just make sure the bib entry is correct and that you don’t overclaim beyond your actual trace encoding.

---

## 4) Extraordinarily detailed improvement plan (text-only, no new results)

This is organized as “what to add,” “where,” and “why a reviewer cares.”

### Phase 0: Tighten the paper’s contract with the reader (1–2 pages of added text total)

1. **Add one paragraph in the Introduction** that explicitly defines the paper’s research problem as *measurement design*:

   * “How do we scale elite attribute coding with retrieval-augmented LLMs without violating auditability, comparability, and temporal validity constraints required for causal inference and descriptive comparisons?”
   * Explicitly say: ASA is a **measurement protocol**, not an accuracy-maximizing QA system.

2. **Add a one-sentence “reproducibility target” definition** in the Roadmap:

   * “Our replication target is recomputation from a frozen trace store and versioned task specification; we do not claim reruns on the live web will match.”

Why: JOP reviewers like clear contracts—what is being claimed and what is not.

---

### Phase 1: Expand the formalism (this is your biggest “completeness” lever)

**Deliverable:** Add Sections \ref{sec:formal-audit-compare} and \ref{sec:selective-measurement} (copy blocks above).

What to ensure:

* **Define the trace (T_i)** explicitly and treat it as the measurement object.
* **Define auditably reproducible** (recompute from frozen traces).
* **Define comparability** as non-expansive mapping into (\mathcal{C}_i).
* **Define coverage and conditional error** for selective measurement.

Why: This makes your paper look like political methodology, not “LLM engineering.”

---

### Phase 2: Make ASA “fully specifiable” (methodological completeness without new experiments)

**Deliverables (add ~2–3 pages total, some can go to appendix if tight on JOP page limit ([Chicago Journals][3])):**

1. **Evidence hierarchy + tiered sufficiency rule (E)**

   * Add the “Tier A/B/C” rule block.
   * Add a short table that lists: evidence tier, minimum sources, source-type requirements, admissible dating requirement, what happens under conflict.

2. **Conflict policy (must be explicit)**
   Add a paragraph:

   * Define what counts as “conflict” (two labels with at least one independent admissible source each).
   * Define tie-breaking vs abstention (default: abstain unless one label strictly dominates by tier + source type).

3. **Date recovery (d(s)): operational details**
   Add a paragraph that lists the ordered heuristics:

   * structured metadata (schema.org `datePublished`, `dateModified`)
   * HTML meta tags, RSS/OG tags
   * visible byline dates
   * URL date patterns (only as weak signal)
   * last-resort: treat as undated
     And explicitly say: undated sources cannot be sole support for a high-evidence label.

4. **Entity disambiguation**
   Add a paragraph defining a disambiguation rule:

   * require at least one corroborating identifier: office title, country, year, or tenure dates in snippet.
   * if ambiguous, abstain.
     This can be purely textual but signals maturity.

5. **Trace schema + auditor checklist**
   Add the trace schema section and optionally a “minimum stored fields” table.

Why: These additions address the “protocol seems under-specified” reviewer critique.

---

### Phase 3: Fix the data section (the most common reason methods papers get rejected as “incomplete”)

**Deliverables:**

1. **Name and cite the leader dataset(s)** and their coverage (years, countries, office types).
2. **Explain record linkage** (even if simple).
3. **Explain codebook construction** and versioning:

   * how you handle coalitions, mergers, aliases
   * whether codebooks are country-year specific (you claim yes)
4. **Explain the expert label process**:

   * who coded, what sources, any adjudication rule
   * (if you cannot report ICR now, at least describe the process and note that ICR is reported in the appendix or future supplement—without inventing numbers)

Why: JOP readers will assume data construction is *the* core contribution; they need to understand inputs.

---

### Phase 4: Strengthen “Related work” so it reads like a JOP paper (not just NLP)

Right now the Related Work is “fine” but too short for how interdisciplinary your claim is.

**Add three mini-paragraphs:**

1. **Political measurement validity** (Adcock & Collier; Munck & Verkuilen; plus one digital trace caution like Lazer et al. 2014). ([PubMed][6])
2. **Transparency infrastructure in political science** (DA-RT; ATI / active citation; practical replication guidance). ([Cambridge University Press & Assessment][7])
3. **Selective prediction / abstention** (Chow; Geifman & El-Yaniv) as your conceptual justification for withholding. ([ACM Digital Library][11])

Why: reviewers in political methodology will want to see you understand the measurement/transparency canon.

---

### Phase 5: Make the validation section interpretation-proof (no new results needed)

**Deliverables:**

1. Add the “Threats to validity in evaluating web-based measurement” subsection (above).
2. Add explicit metric formulas (even if simple).
3. Add an “error taxonomy” paragraph *without inventing counts*:

   * ambiguity/homonyms
   * within-year party switches
   * coalition vs party name mismatch
   * sources without dates
   * translation/transliteration variants
     and for each, say which protocol component addresses it (temporal cut, codebook mapping, abstention, etc.).

Why: This makes your empirical section feel like it has “closure” even without new ablations in the main text.

---

### Phase 6: Upgrade Transparency/Replication to meet JOP expectations

Given JOP replication expectations are explicit (README, datasets, codebook, code; Dataverse upload), you should mirror that language. ([csdp.princeton.edu][1])

**Deliverables:**

1. Add a “Replication package contents” bullet list (copy above).
2. Add a paragraph on “frozen trace store snapshot” as the unit of reproducibility.
3. Add a web persistence paragraph:

   * link rot is common (cite Klein et al. 2014) ([PLOS][14])
   * use robust links / snapshots where possible (Memento RFC 7089) ([IETF Datatracker][15])
   * store hashes + bounded snippets.

Why: This is *exactly* where JOP reviewers probe.

---

### Phase 7: Deepen Limitations/Ethics with politically-relevant concerns

Add 2–3 paragraphs (no new results) that feel “complete”:

* **Coverage bias as a selection mechanism** (who gets withheld and why)
* **Language and regional asymmetries** in web evidence
* **Legal/TOS constraints** and how trace storage minimizes risk (store snippets, not full pages)
* **Sensitive attributes**: reaffirm your scope condition that ASA is for verifiable attributes; avoid coding sensitive traits without self-identification.

Why: It anticipates normative reviewer concerns without changing results.

---

## 5) Two “quick wins” that are almost always worth it

1. **Add a one-page “ASA specification table”** (main text or appendix) with columns:

   * Parameter (e.g., (B), (\tau_i), (E), mapper threshold, minimum independent sources)
   * Party-labeling value used in this paper
   * Rationale
     This instantly makes the protocol look mature and reproducible.

2. **Add a “Worked audit example” that shows a real audit step-by-step**
   You already have a worked example box, but it’s conceptual. Add a second box in the appendix that says:

* here is the record id
* here are the stored citations
* here is the recovered date and why it’s admissible
* here is the mapping into codebook
* here is why alternatives are rejected
  No new computation needed—you already have sample predictions tables; just narrate one.

---

## 6) Journal-of-Politics-specific positioning reminder (textual, not empirical)

Because initial submissions as research articles are constrained in length (commonly described as ~35 pages including references/tables/figures), you should keep new main-text additions tight and move “schema-level” detail to the appendix. ([Chicago Journals][3])

In practice:

* Add **the formal definitions** in the main text (they are “theory,” not “engineering”).
* Put **trace schema field lists, mapping heuristics, and date-extraction heuristics** in the appendix.

---


[1]: https://csdp.princeton.edu/sites/g/files/toruqf2376/files/documents/Journal%20Reference%20Guide%209_26_24.pdf "https://csdp.princeton.edu/sites/g/files/toruqf2376/files/documents/Journal%20Reference%20Guide%209_26_24.pdf"
[2]: https://www.arxiv.org/pdf/2602.13855 "From Fluent to Verifiable: Claim-Level Auditability for Deep Research Agents"
[3]: https://www.journals.uchicago.edu/journals/jop/instruct "https://www.journals.uchicago.edu/journals/jop/instruct"
[4]: https://journals.sagepub.com/doi/10.1177/001041400203500101 "https://journals.sagepub.com/doi/10.1177/001041400203500101"
[5]: https://www.ucg.ac.me/skladiste/blog_609772/objava_106827/fajlovi/Conceptualizing%20and%20measuring%20democracy%20_%20A%20new%20approach.pdf "https://www.ucg.ac.me/skladiste/blog_609772/objava_106827/fajlovi/Conceptualizing%20and%20measuring%20democracy%20_%20A%20new%20approach.pdf"
[6]: https://pubmed.ncbi.nlm.nih.gov/24626916/ "https://pubmed.ncbi.nlm.nih.gov/24626916/"
[7]: https://resolve.cambridge.org/core/journals/european-journal-of-political-research/article/data-access-and-research-transparency-dart-a-joint-statement-by-political-science-journal-editors/3C125D9141FEC378AC8B26BA78FF6AA8 "Data access and research transparency (DA‐RT): A joint statement by Political Science Journal Editors | European Journal of Political Research | Cambridge Core"
[8]: https://www.princeton.edu/~amoravcs/library/transparency.pdf "https://www.princeton.edu/~amoravcs/library/transparency.pdf"
[9]: https://qdr.syr.edu/node/20637 "https://qdr.syr.edu/node/20637"
[10]: https://web.stanford.edu/~gentzkow/research/CodeAndData.pdf "https://web.stanford.edu/~gentzkow/research/CodeAndData.pdf"
[11]: https://dl.acm.org/doi/10.1109/TIT.1970.1054406 "https://dl.acm.org/doi/10.1109/TIT.1970.1054406"
[12]: https://arxiv.org/abs/1705.08500 "https://arxiv.org/abs/1705.08500"
[13]: https://arxiv.org/abs/2107.07511 "https://arxiv.org/abs/2107.07511"
[14]: https://journals.plos.org/plosone/article?id=10.1371%2Fjournal.pone.0115253 "https://journals.plos.org/plosone/article?id=10.1371%2Fjournal.pone.0115253"
[15]: https://datatracker.ietf.org/doc/rfc7089/ "https://datatracker.ietf.org/doc/rfc7089/"
[16]: https://harvardlawreview.org/wp-content/uploads/2014/03/forvol127_zittrain.pdf "https://harvardlawreview.org/wp-content/uploads/2014/03/forvol127_zittrain.pdf"
[17]: https://www.cs.cornell.edu/~shmat/courses/cs6434/fellegi-sunter.pdf "https://www.cs.cornell.edu/~shmat/courses/cs6434/fellegi-sunter.pdf"
[18]: https://journals.sagepub.com/doi/10.1177/0022343308100719 "https://journals.sagepub.com/doi/10.1177/0022343308100719"
[19]: https://academic.oup.com/wber/article-abstract/15/1/165/1682021 "https://academic.oup.com/wber/article-abstract/15/1/165/1682021"
[20]: https://journals.sagepub.com/doi/abs/10.1177/1354068818820671 "https://journals.sagepub.com/doi/abs/10.1177/1354068818820671"
[21]: https://www.parlgov.org/about/ "https://www.parlgov.org/about/"
[22]: https://www.v-dem.net/documents/6/vparty_codebook_v2.pdf "https://www.v-dem.net/documents/6/vparty_codebook_v2.pdf"
[23]: https://www.frontiersin.org/journals/political-science/articles/10.3389/fpos.2023.1257092/full "https://www.frontiersin.org/journals/political-science/articles/10.3389/fpos.2023.1257092/full"
