USE [DataProcessingAI]
GO
/****** Object:  Schema [Gen]    Script Date: 10/27/2023 10:15:15 AM ******/
CREATE SCHEMA [Gen]
GO
/****** Object:  Schema [Process]    Script Date: 10/27/2023 10:15:15 AM ******/
CREATE SCHEMA [Process]
GO
/****** Object:  Table [Gen].[SerialGenerator]    Script Date: 10/27/2023 10:15:15 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [Gen].[SerialGenerator](
	[Attribute] [nvarchar](50) NOT NULL,
	[Value] [bigint] NOT NULL,
	[Year] [int] NULL
) ON [PRIMARY]
GO
/****** Object:  Table [Gen].[Tasks]    Script Date: 10/27/2023 10:15:15 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [Gen].[Tasks](
	[TaskID] [int] NOT NULL,
	[Description] [nvarchar](50) NOT NULL,
 CONSTRAINT [PK_Tasks] PRIMARY KEY CLUSTERED 
(
	[TaskID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  Table [Process].[DataFiles]    Script Date: 10/27/2023 10:15:15 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [Process].[DataFiles](
	[FileID] [bigint] NOT NULL,
	[FileName] [nvarchar](250) NOT NULL,
	[FileExtenstion] [nvarchar](50) NOT NULL,
	[FileContent] [varbinary](max) NULL,
	[FileSize] [int] NOT NULL,
	[FilePath] [nvarchar](1000) NULL,
	[RowsCount] [int] NULL,
	[ColumnsCount] [int] NULL,
	[CreatedBy] [int] NULL,
	[CreatedOn] [datetime] NULL,
	[ModifiedBy] [int] NULL,
	[ModifiedOn] [datetime] NULL,
	[StatusID] [int] NULL,
	[IsActive] [bit] NOT NULL,
 CONSTRAINT [PK_DataFiles] PRIMARY KEY CLUSTERED 
(
	[FileID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
/****** Object:  Table [Process].[DataFilesTasks]    Script Date: 10/27/2023 10:15:15 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [Process].[DataFilesTasks](
	[ID] [bigint] IDENTITY(1,1) NOT NULL,
	[FileID] [bigint] NOT NULL,
	[TaskID] [int] NOT NULL,
	[TaskStatus] [int] NOT NULL,
	[UpdatedOn] [datetime] NULL,
 CONSTRAINT [PK_DataFilesTasks] PRIMARY KEY CLUSTERED 
(
	[ID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  Table [Process].[DataFilesTemp]    Script Date: 10/27/2023 10:15:15 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [Process].[DataFilesTemp](
	[BinaryData] [varbinary](max) NOT NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
ALTER TABLE [Process].[DataFiles] ADD  CONSTRAINT [DF_DataFiles_IsActive]  DEFAULT ((1)) FOR [IsActive]
GO
ALTER TABLE [Process].[DataFilesTasks] ADD  CONSTRAINT [DF_Table_1_Completed]  DEFAULT ((0)) FOR [TaskStatus]
GO
/****** Object:  StoredProcedure [Process].[SaveDataFilesTasks]    Script Date: 10/27/2023 10:15:15 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		<Author,,Name>
-- Create date: <Create Date,,>
-- Description:	<Description,,>
-- =============================================
CREATE PROCEDURE [Process].[SaveDataFilesTasks] 
	@FileID BIGINT
AS
BEGIN
	INSERT INTO [Process].[DataFilesTasks]
    ([TaskID]
	,[FileID]
    ,[TaskStatus])
	SELECT [TaskID],@FileID,0
	FROM [DataProcessingAI].[Gen].[Tasks]
END
GO
/****** Object:  StoredProcedure [Process].[UpdateDataFileTask]    Script Date: 10/27/2023 10:15:15 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		<Author,,Name>
-- Create date: <Create Date,,>
-- Description:	<Description,,>
-- =============================================
CREATE PROCEDURE [Process].[UpdateDataFileTask]
	@FileID BIGINT,
	@TaskID INT,
	@TaskStatus INT = 1 --1 - Processed, 2 - Not Applicable
AS
BEGIN
	UPDATE [Process].[DataFilesTasks]
	SET TaskStatus = @TaskStatus, UpdatedOn = GETDATE()
	WHERE FileID = @FileID and TaskID = @TaskID

	IF (SELECT COUNT(DFT.ID)
	FROM Process.DataFilesTasks DFT
	INNER JOIN Process.DataFiles DF ON DFT.FileID = DF.FileID	
	WHERE DF.FileID = @FileID AND DFT.TaskStatus IN (1,2) AND DFT.TaskID IN (1,2,3,8)) = 4
		BEGIN
			UPDATE Process.DataFiles
			SET StatusID = 2
			WHERE FileID = @FileID
		END

END
GO
