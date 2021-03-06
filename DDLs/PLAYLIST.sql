CREATE TABLE PLAYLIST(
	[PLAYLIST_NAME] [nvarchar](255) NULL,
	[PLAYLIST_ID] [nvarchar](100) NOT NULL,
	[COLLABORATIVE] [nchar](10) NULL,
	[SNAPSHOT_ID] [nvarchar](100) NOT NULL,
	[Description] [nchar](255) NULL,
	[HREF] [nvarchar](255) NULL,
	[PUB] [nchar](10) NULL,
	[URI] [nvarchar](255) NULL,
	[USER_ID] [nvarchar](50) NULL,
 CONSTRAINT [PK_PLAYLIST] PRIMARY KEY CLUSTERED 
(
	[PLAYLIST_ID] ASC,
	[SNAPSHOT_ID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON)
)