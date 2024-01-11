﻿USE [master]
GO
CREATE LOGIN [user_etl] WITH PASSWORD=N'demopass', DEFAULT_DATABASE=[AdventureWorksDW2019], CHECK_EXPIRATION=OFF, CHECK_POLICY=OFF
GO
USE [AdventureWorksDW2019]
GO
CREATE USER [user_etl] FOR LOGIN [user_etl]
GO
USE [AdventureWorksDW2019]
GO
ALTER ROLE [db_datareader] ADD MEMBER [user_etl]
GO
use [master]
GO
GRANT CONNECT SQL TO [user_etl]
GO