-- One row per real article, with the NULL structural lines above it folded in.
-- grp = idx_element of the next non-NULL article at or after the current line.

SET SESSION group_concat_max_len = 8192;

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
        MIN(CASE WHEN a.article_id IS NOT NULL THEN a.idx_element END)
            OVER (PARTITION BY m.id
                  ORDER BY a.idx_element
                  ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS grp
    FROM caisse_mouvement m
    JOIN caisse_mouvement_article a ON a.mvm_caisse_id = m.id
    LEFT JOIN `user` u ON u.id = m.user_id
    WHERE DATE(m.{date_col}) = '{date}'
      AND (m.is_annule IS NULL OR m.is_annule = 0)
      AND (a.is_annule IS NULL OR a.is_annule = 0)
)
SELECT
    ticket_id                                            AS VTE_ORDRE,
    dt                                                   AS VTE_DATE_HEURE,
    TIME(dt)                                             AS VTE_HEURE,
    usr                                                  AS USR_NOM,
    grp                                                  AS LIGNE_IDX,

    MAX(CASE WHEN idx = grp THEN article_id END)         AS ART_ID,
    MAX(CASE WHEN idx = grp THEN libelle END)            AS ART_LIBELLE,

    -- the folded-in headers and modifiers, in register order
    GROUP_CONCAT(CASE WHEN idx < grp THEN libelle END
                 ORDER BY idx SEPARATOR '-')             AS CONTEXTE,

    -- full path, e.g. "BOISSON CHAUD-AVEC EAU-CAFE NOIR"
    GROUP_CONCAT(libelle ORDER BY idx SEPARATOR '-')     AS CHEMIN,

    MAX(CASE WHEN idx = grp THEN quantite END)           AS VTE_QUANTITE,

    ROUND(SUM(mtt_total), 2)                             AS TOTAL_TTC,
    ROUND(SUM(mtt_total)
          / NULLIF(MAX(CASE WHEN idx = grp THEN quantite END), 0), 2)
                                                         AS VTE_PRIX_DE_VENTE,
    ROUND(SUM(mtt_total) / (1 + {tva}/100), 2)           AS TOTAL_HT,
    ROUND(SUM(mtt_total)
          - SUM(mtt_total) / (1 + {tva}/100), 2)         AS TOTAL_TVA

FROM lignes
GROUP BY ticket_id, dt, usr, grp
ORDER BY dt, ticket_id, grp;


-- ---------------------------------------------------------------------------
-- Variant: keep priced formula lines as their own row instead of folding
-- their price onto the included drink. Change only the window expression:
--
--     MIN(CASE WHEN a.article_id IS NOT NULL OR a.mtt_total <> 0
--              THEN a.idx_element END)
--         OVER (PARTITION BY m.id ORDER BY a.idx_element
--               ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS grp
--
-- Every other line of the query stays identical.
-- ---------------------------------------------------------------------------