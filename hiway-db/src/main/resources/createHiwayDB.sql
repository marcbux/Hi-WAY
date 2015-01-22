SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0;
SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0;
SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='TRADITIONAL,ALLOW_INVALID_DATES';

DROP SCHEMA IF EXISTS `hiwaydb` ;
CREATE SCHEMA IF NOT EXISTS `hiwaydb` DEFAULT CHARACTER SET utf8 COLLATE utf8_general_ci ;
SHOW WARNINGS;
USE `hiwaydb` ;

-- -----------------------------------------------------
-- Table `Workflowrun`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `Workflowrun` ;

SHOW WARNINGS;
CREATE TABLE IF NOT EXISTS `Workflowrun` (
  `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  `runid` VARCHAR(50) NOT NULL,
  `wfname` TEXT NULL,
  `wftime` BIGINT UNSIGNED NULL,
  PRIMARY KEY (`id`),
  UNIQUE INDEX `RundID_UNIQUE` (`runid` ASC))
ENGINE = InnoDB;

SHOW WARNINGS;

-- -----------------------------------------------------
-- Table `Task`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `Task` ;

SHOW WARNINGS;
CREATE TABLE IF NOT EXISTS `Task` (
  `taskid` BIGINT UNSIGNED NOT NULL,
  `taskname` TEXT NULL,
  `language` TEXT NOT NULL,
  PRIMARY KEY (`taskid`))
ENGINE = InnoDB;

SHOW WARNINGS;

-- -----------------------------------------------------
-- Table `Invocation`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `Invocation` ;

SHOW WARNINGS;
CREATE TABLE IF NOT EXISTS `Invocation` (
  `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  `Invocationid` BIGINT NOT NULL,
  `hostname` VARCHAR(250) NULL,
  `standardout` LONGTEXT NULL,
  `standarderror` LONGTEXT NULL,
  `scheduleTime` BIGINT UNSIGNED NULL,
  `DidOn` TIMESTAMP NOT NULL,
  `realtime` BIGINT UNSIGNED NULL,
  `realtimein` BIGINT UNSIGNED NULL,
  `realtimeout` BIGINT UNSIGNED NULL,
  `Workflowrun_id` BIGINT UNSIGNED NOT NULL,
  `Task_taskid` BIGINT UNSIGNED NOT NULL,
  `Timestamp` BIGINT UNSIGNED NULL,
  PRIMARY KEY (`id`),
  UNIQUE INDEX `InvocInRun` (`Invocationid` ASC, `Workflowrun_id` ASC),
  INDEX `fk_Invocation_Workflowrun1_idx` (`Workflowrun_id` ASC),
  INDEX `fk_Invocation_Task1_idx` (`Task_taskid` ASC),
  CONSTRAINT `fk_Invocation_Workflowrun1`
    FOREIGN KEY (`Workflowrun_id`)
    REFERENCES `Workflowrun` (`id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `fk_Invocation_Task1`
    FOREIGN KEY (`Task_taskid`)
    REFERENCES `Task` (`taskid`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)
ENGINE = InnoDB;

SHOW WARNINGS;

-- -----------------------------------------------------
-- Table `File`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `File` ;

SHOW WARNINGS;
CREATE TABLE IF NOT EXISTS `File` (
  `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  `name` VARCHAR(250) NOT NULL,
  `size` BIGINT UNSIGNED NULL,
  `realtimein` BIGINT UNSIGNED NULL,
  `realtimeout` BIGINT UNSIGNED NULL,
  `Invocation_id` BIGINT UNSIGNED NOT NULL,
  PRIMARY KEY (`id`),
  INDEX `fk_File_Invocation1_idx` (`Invocation_id` ASC),
  UNIQUE INDEX `JustOneFile` (`name` ASC, `Invocation_id` ASC),
  CONSTRAINT `fk_File_Invocation1`
    FOREIGN KEY (`Invocation_id`)
    REFERENCES `Invocation` (`id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)
ENGINE = InnoDB;

SHOW WARNINGS;

-- -----------------------------------------------------
-- Table `Inoutput`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `Inoutput` ;

SHOW WARNINGS;
CREATE TABLE IF NOT EXISTS `Inoutput` (
  `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  `keypart` TEXT NOT NULL,
  `content` LONGTEXT NOT NULL,
  `type` VARCHAR(45) NOT NULL,
  `Invocation_id` BIGINT UNSIGNED NOT NULL,
  PRIMARY KEY (`id`),
  INDEX `fk_Inoutput_Invocation1_idx` (`Invocation_id` ASC),
  CONSTRAINT `fk_Inoutput_Invocation1`
    FOREIGN KEY (`Invocation_id`)
    REFERENCES `Invocation` (`id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)
ENGINE = InnoDB;

SHOW WARNINGS;

-- -----------------------------------------------------
-- Table `Userevent`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `Userevent` ;

SHOW WARNINGS;
CREATE TABLE IF NOT EXISTS `Userevent` (
  `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  `content` LONGTEXT NOT NULL,
  `Invocation_id` BIGINT UNSIGNED NOT NULL,
  PRIMARY KEY (`id`),
  INDEX `fk_Userevent_Invocation1_idx` (`Invocation_id` ASC),
  CONSTRAINT `fk_Userevent_Invocation1`
    FOREIGN KEY (`Invocation_id`)
    REFERENCES `Invocation` (`id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)
ENGINE = InnoDB;

SHOW WARNINGS;

-- -----------------------------------------------------
-- Table `Hiwayevent`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `Hiwayevent` ;

SHOW WARNINGS;
CREATE TABLE IF NOT EXISTS `Hiwayevent` (
  `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  `content` LONGTEXT NOT NULL,
  `type` VARCHAR(200) NOT NULL,
  `Workflowrun_id` BIGINT UNSIGNED NOT NULL,
  PRIMARY KEY (`id`),
  INDEX `fk_Hiwayevent_Workflowrun1_idx` (`Workflowrun_id` ASC),
  CONSTRAINT `fk_Hiwayevent_Workflowrun1`
    FOREIGN KEY (`Workflowrun_id`)
    REFERENCES `Workflowrun` (`id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)
ENGINE = InnoDB;

SHOW WARNINGS;

SET SQL_MODE=@OLD_SQL_MODE;
SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS;
SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS;
