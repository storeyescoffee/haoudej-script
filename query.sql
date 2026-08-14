-- One row per billable line. Structural NULL lines fold into the line below.
-- Priced lines anchor their own group, so breakfast formulas stay separate
-- from the drink they include.
--
-- NOTE: run this SET on its own execute(), not concatenated with the SELECT --
-- the connector rejects multi-statement and reports the error at line 6.
--   SET SESSION group_concat_max_len = 8192;

WITH lignes AS (
    SELECT
        m.id                AS ticket_id,
        m.{date_col}        AS dt,
        u.login             AS usr,
        a.idx_element       AS idx,
        a.article_id,
        a.libelle,
        a.quantite,
        a.mtt_total,
        MIN(CASE WHEN a.article_id IS NOT NULL OR a.mtt_total <> 0
                 THEN a.idx_element END)
            OVER (PARTITION BY m.id
                  ORDER BY a.idx_element
                  ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS grp
    FROM caisse_mouvement m
    JOIN caisse_mouvement_article a ON a.mvm_caisse_id = m.id
    LEFT JOIN `user` u ON u.id = m.user_id
    WHERE DATE(m.{date_col}) = '{date}'
      AND (m.is_annule IS NULL OR m.is_annule = 0)
      AND (a.is_annule IS NULL OR a.is_annule = 0)
),
groupes AS (
    SELECT l.*, MIN(idx) OVER (PARTITION BY ticket_id, grp) AS idx_debut
    FROM lignes l
)
SELECT
    ticket_id                                            AS VTE_ORDRE,
    dt                                                   AS VTE_DATE_HEURE,
    TIME(dt)                                             AS VTE_HEURE,
    usr                                                  AS USR_NOM,
    grp                                                  AS LIGNE_IDX,

    MAX(CASE WHEN idx = grp THEN article_id END)         AS ART_ID,

    -- article first, then its folded-in lines:
    --   "CAFE NOIR - BOISSON CHAUD - AVEC EAU"
    -- CONCAT_WS drops the NULL, so a context-less line is just "CAFE NOIR"
    CONCAT_WS(' - ',
        MAX(CASE WHEN idx = grp THEN libelle END),
        GROUP_CONCAT(CASE WHEN idx < grp THEN libelle END
                     ORDER BY idx SEPARATOR ' - ')
    )                                                    AS ART_LIBELLE,

    GROUP_CONCAT(CASE WHEN idx < grp THEN libelle END
                 ORDER BY idx SEPARATOR ' - ')           AS CONTEXTE,

    -- to drop the leading category and get "CAFE NOIR - AVEC EAU", swap the
    -- GROUP_CONCAT condition above for:
    --     CASE WHEN idx < grp AND idx > idx_debut THEN libelle END

    MAX(CASE WHEN idx = grp THEN quantite END)           AS VTE_QUANTITE,

    ROUND(SUM(mtt_total), 2)                             AS TOTAL_TTC,
    ROUND(SUM(mtt_total)
          / NULLIF(MAX(CASE WHEN idx = grp THEN quantite END), 0), 2)
                                                         AS VTE_PRIX_DE_VENTE,
    ROUND(SUM(mtt_total) / (1 + {tva}/100), 2)           AS TOTAL_HT,
    ROUND(SUM(mtt_total)
          - SUM(mtt_total) / (1 + {tva}/100), 2)         AS TOTAL_TVA

FROM groupes
GROUP BY ticket_id, dt, usr, grp
ORDER BY dt, ticket_id, grp;