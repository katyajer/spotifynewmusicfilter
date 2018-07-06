CREATE TABLE TRACK(
	[TRACK_NAME] [nvarchar](255) NOT NULL,
	[TRACK_ID] [nvarchar](50) NOT NULL,
	[duration_ms] [int] NULL,
	[EXPLICIT] [nchar](10) NULL,
	[HREF] [nvarchar](max) NULL,
	[POPULARITY] [int] NULL,
	[URI] [nvarchar](255) NULL,
	[ALBUM_ID] [nvarchar](50) NULL,
	[ARTIST_ID] [nvarchar](50) NULL,
	[ARTIST_ID2] [nvarchar](50) NULL,
	[ARTIST_ID3] [nvarchar](50) NULL,
	[ARTIST_ID4] [nvarchar](50) NULL,
PRIMARY KEY CLUSTERED 
(
	[TRACK_ID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON)
)