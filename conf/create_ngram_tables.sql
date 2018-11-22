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
DROP TABLE IF EXISTS `cndblp_title_ngram`;
CREATE TABLE `cndblp_title_ngram`  (
  `paper_id` int(11) NOT NULL,
  `ngram` bigint(11) NOT NULL,
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
  `ngram` bigint(11) NULL DEFAULT NULL,
  INDEX `paper_id`(`paper_id`) USING BTREE,
  INDEX `ngram`(`ngram`) USING BTREE,
  INDEX `p_n_com`(`paper_id`, `ngram`) USING BTREE
) ENGINE = InnoDB CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;

SET FOREIGN_KEY_CHECKS = 1;