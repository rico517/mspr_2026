-- Suppression si existante
DROP DATABASE IF EXISTS elections_db;
CREATE DATABASE elections_db CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
USE elections_db;

-- ==========================
-- TABLE : scrutins
-- ==========================
CREATE TABLE scrutins (
    id INT AUTO_INCREMENT PRIMARY KEY,
    type VARCHAR(100) NOT NULL,
    annee INT NOT NULL,
    tour INT NOT NULL
) ENGINE=InnoDB;

-- ==========================
-- TABLE : bords_politique
-- ==========================
CREATE TABLE bords_politiques (
    id INT AUTO_INCREMENT PRIMARY KEY,
    label VARCHAR(100) NOT NULL UNIQUE
) ENGINE=InnoDB;

-- ==========================
-- TABLE : candidats
-- ==========================
CREATE TABLE candidats (
    id INT AUTO_INCREMENT PRIMARY KEY,
    id_bord_politique INT NOT NULL,
    nom VARCHAR(100) NOT NULL,
    prenom VARCHAR(100) NOT NULL,
    CONSTRAINT fk_candidat_bord
        FOREIGN KEY (id_bord_politique)
        REFERENCES bords_politiques(id)
        ON DELETE RESTRICT
        ON UPDATE CASCADE
) ENGINE=InnoDB;

ALTER TABLE candidats ADD UNIQUE (nom, prenom);

-- ==========================
-- TABLE : circonscriptions
-- ==========================
CREATE TABLE circonscriptions (
    id INT AUTO_INCREMENT PRIMARY KEY,
    code INT NOT NULL    
) ENGINE=InnoDB;

-- ==========================
-- TABLE : scrutin_circonscription
-- ==========================

CREATE TABLE scrutins_circonscriptions (
    id INT AUTO_INCREMENT PRIMARY KEY,
    id_scrutin INT NOT NULL,
    id_circonscription INT NOT NULL,
    inscrits INT NOT NULL DEFAULT 0,
    abstentions INT NOT NULL DEFAULT 0,
    votants INT NOT NULL DEFAULT 0,
    exprimes INT NOT NULL DEFAULT 0,
    blancs_nuls INT NOT NULL DEFAULT 0,

    UNIQUE (id_scrutin, id_circonscription),

    CONSTRAINT fk_sc_scrutin
        FOREIGN KEY (id_scrutin)
        REFERENCES scrutins(id)
        ON DELETE RESTRICT
        ON UPDATE CASCADE,

    CONSTRAINT fk_sc_circonscription
        FOREIGN KEY (id_circonscription)
        REFERENCES circonscriptions(id)
        ON DELETE RESTRICT
        ON UPDATE CASCADE
) ENGINE=InnoDB;

-- ==========================
-- TABLE : votes
-- ==========================
CREATE TABLE votes (
    id INT AUTO_INCREMENT PRIMARY KEY,
    id_candidat INT NOT NULL,
    id_scrutin_circonscription INT NOT NULL,
    voix INT NOT NULL DEFAULT 0,
    CONSTRAINT fk_vote_candidat
        FOREIGN KEY (id_candidat)
        REFERENCES candidats(id)
        ON DELETE CASCADE
        ON UPDATE CASCADE,
    CONSTRAINT fk_vote_scrut_circo
        FOREIGN KEY (id_scrutin_circonscription)
        REFERENCES scrutins_circonscriptions(id)
        ON DELETE CASCADE
        ON UPDATE CASCADE
) ENGINE=InnoDB;