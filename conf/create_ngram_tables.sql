/*
 Navicat Premium Data Transfer

 Source Server         : localhost
 Source Server Type    : MySQL
 Source Server Version : 50723
 Source Host           : localhost:3306
 Source Schema         : patent_thesis

 Target Server Type    : MySQL
 Target Server Version : 50723
 File Encoding         : 65001

 Date: 21/11/2018 10:31:25
*/

SET group_concat_max_len=15000;

UPDATE `cndblp_reference` SET `DOI` = SUBSTR(`DOI`, 5) WHERE `DOI` LIKE 'DOI%';
UPDATE `cndblp_paper` SET `DOI` = SUBSTR(`DOI`, 5) WHERE `DOI` LIKE 'DOI%';
UPDATE `cndblp_reference` SET `DOI` = UPPER(`DOI`);
UPDATE `cndblp_paper` SET `DOI` = UPPER(`DOI`);

SET NAMES utf8mb4;
SET FOREIGN_KEY_CHECKS = 0;

-- ----------------------------
-- Table structure for cndblp_ngram_list
-- ----------------------------
DROP TABLE IF EXISTS `cndblp_ngram_list`;
CREATE TABLE `cndblp_ngram_list`  (
  `nid` int(11) NOT NULL,
  `ngram` varchar(400) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  PRIMARY KEY (`nid`) USING BTREE,
  INDEX `nid`(`nid`) USING BTREE,
  INDEX `ngram`(`ngram`) USING BTREE,
  INDEX `n_n_com`(`nid`, `ngram`) USING BTREE
) ENGINE = InnoDB CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;

SET FOREIGN_KEY_CHECKS = 1;

-- ----------------------------
-- Table structure for cndblp_title_ngram
-- ----------------------------
DROP TABLE IF EXISTS `cndblp_title_keyword_ngram`;
CREATE TABLE `cndblp_title_keyword_ngram`  (
  `paper_id` int(11) NOT NULL,
  `ngram` int(11) NOT NULL,
  INDEX `paper_id`(`paper_id`) USING BTREE,
  INDEX `ngram`(`ngram`) USING BTREE,
  INDEX `p_n_com`(`paper_id`, `ngram`) USING BTREE
) ENGINE = InnoDB CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;

SET FOREIGN_KEY_CHECKS = 1;

-- ----------------------------
-- Table structure for cndblp_abs_ngram
-- ----------------------------
DROP TABLE IF EXISTS `cndblp_abs_ngram`;
CREATE TABLE `cndblp_abs_ngram`  (
  `paper_id` int(11) NOT NULL,
  `ngram` int(11) NULL DEFAULT NULL,
  INDEX `paper_id`(`paper_id`) USING BTREE,
  INDEX `ngram`(`ngram`) USING BTREE,
  INDEX `p_n_com`(`paper_id`, `ngram`) USING BTREE
) ENGINE = InnoDB CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;

SET FOREIGN_KEY_CHECKS = 1;

-- ----------------------------
-- Table structure for cndblp_keyword_ngram
-- ----------------------------
DROP TABLE IF EXISTS `cndblp_keyword_ngram`;
CREATE TABLE `cndblp_keyword_ngram`  (
  `paper_id` int(11) NOT NULL,
  `ngram` int(11) NULL DEFAULT NULL,
  INDEX `paper_id`(`paper_id`) USING BTREE,
  INDEX `ngram`(`ngram`) USING BTREE,
  INDEX `p_n_com`(`paper_id`, `ngram`) USING BTREE
) ENGINE = InnoDB CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;

SET FOREIGN_KEY_CHECKS = 1;

-- ----------------------------
-- Table structure for cndblp_inner_reference
-- ----------------------------
DROP TABLE IF EXISTS `cndblp_inner_reference`;
CREATE TABLE `cndblp_inner_reference`  (
  `paper_id` int(11) UNSIGNED NOT NULL DEFAULT 0,
  `cited_paper_id` int(11) UNSIGNED NOT NULL DEFAULT 0,
  INDEX `paper_id`(`paper_id`) USING BTREE,
  INDEX `cited_paper_id`(`cited_paper_id`) USING BTREE,
  INDEX `p_c_com`(`paper_id`, `cited_paper_id`) USING BTREE
) ENGINE = InnoDB CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;

SET FOREIGN_KEY_CHECKS = 1;


INSERT INTO `cndblp_inner_reference`
  SELECT CAST(a.`PAPER_ID` AS UNSIGNED) AS `paper_id`,
         CAST(b.`PAPER_ID` AS UNSIGNED) AS `cited_paper_id`
  FROM `cndblp_reference` AS a
  INNER JOIN cndblp_paper AS b
  ON a.DOI = b.DOI
  WHERE a.DOI IS NOT NULL
  ORDER BY paper_id, cited_paper_id;