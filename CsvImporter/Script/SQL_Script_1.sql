
DROP SCHEMA  IF EXISTS  master  ;
GO
CREATE SCHEMA master  ;
GO


IF OBJECT_ID('master.dbo.Stock', 'U') IS NOT NULL 
  DROP TABLE master.dbo.Stock; 
GO

CREATE TABLE master.dbo.Stock
	(
	PointOfSale nvarchar(50) NULL,
	Product nvarchar(50) NOT NULL,
	Date datetime2(7) NULL,
	Stock int NOT NULL
	)  ON [PRIMARY]
GO
USE [master]
GO
/****** Object:  StoredProcedure [dbo].[insertStock]    Script Date: 4/9/2021 11:11:24 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

  CREATE OR ALTER   PROCEDURE  [dbo].[insertStock]
@PointOfSale numeric(18,0),  
@Product NVARCHAR(50),  
@Date NVARCHAR(20),
@Stock int,
@REPETIDO INT OUT
AS    
Begin


SELECT @REPETIDO = 1 FROM dbo.Stock WHERE
PointOfSale=@PointOfSale AND
Product=@Product  AND
[Date]=@Date  AND
Stock=@Stock  

BEGIN
IF(@REPETIDO>0)
DELETE FROM dbo.Stock WHERE
PointOfSale=@PointOfSale AND
Product=@Product  AND
[Date]=@Date  AND
Stock=@Stock  
ELSE
SET @REPETIDO=0;
END

INSERT INTO dbo.Stock values (@PointOfSale,@Product,CONVERT(DATETIME, @Date)  ,@Stock);


END

--select * from dbo.Stock 
--insert into dbo.Stock values (1,2,convert(DATETIME, '2011-11-09 00:00:00') ,3);
--exec insertStock 0898,345351,'2011-11-09 00:00:00',1 ;


