SELECT 
sv.grd__pid, grd.pid, sv.grd__starttime, grd.starttime 
FROM grd
JOIN (
	SELECT ST_Collect(posi_poly__poly::geometry) as poly, grd__starttime, grd__pid FROM superview
	WHERE grd__pid IN (
		'S1A_IW_GRDH_1SDV_20200724T020713_20200724T020738_033590_03E494_B420',
'S1A_IW_GRDH_1SDV_20200727T053553_20200727T053622_033636_03E5FE_2D39',
'S1A_IW_GRDH_1SDV_20200729T034859_20200729T034924_033664_03E6D3_93EF',
'S1A_IW_GRDH_1SDV_20200729T095401_20200729T095430_033668_03E6EE_2611',
'S1A_IW_GRDH_1SDV_20200802T062802_20200802T062827_033724_03E89F_6485',
'S1A_IW_GRDH_1SDV_20200805T220025_20200805T220050_033777_03EA60_3031',
'S1A_IW_GRDH_1SDV_20200806T152803_20200806T152828_033788_03EAB9_DCCA',
'S1A_IW_GRDH_1SDV_20200806T223947_20200806T224012_033792_03EAE3_9E54',
'S1A_IW_GRDH_1SDV_20200808T222606_20200808T222633_033821_03EBDD_2485',
'S1A_IW_GRDH_1SDV_20200811T060256_20200811T060321_033855_03ED02_07D6',
'S1A_IW_GRDH_1SDV_20200811T060641_20200811T060706_033855_03ED02_3E59',
'S1A_IW_GRDH_1SDV_20200812T125006_20200812T125026_033874_03EDB4_721F',
'S1A_IW_GRDH_1SDV_20200814T105729_20200814T105754_033902_03EE9F_3456',
'S1A_IW_GRDH_1SDV_20200816T061108_20200816T061133_033928_03EF95_6194',
'S1A_IW_GRDH_1SDV_20200817T163009_20200817T163025_033949_03F053_C800',
'S1A_IW_GRDH_1SDV_20200817T163009_20200817T163034_033949_03F053_8FD9',
'S1A_IW_GRDH_1SDV_20200819T062057_20200819T062126_033972_03F134_D4C1',
'S1A_IW_GRDH_1SDV_20200819T142908_20200819T142933_033977_03F162_C177',
'S1A_IW_GRDH_1SDV_20200820T165606_20200820T165631_033993_03F1E0_DF57',
'S1A_IW_GRDH_1SDV_20200826T062739_20200826T062804_034074_03F4B5_151E',
'S1A_IW_GRDH_1SDV_20200826T174457_20200826T174524_034081_03F4F8_1812',
'S1A_IW_GRDH_1SDV_20200828T061109_20200828T061134_034103_03F5BD_EBE2',
'S1A_IW_GRDH_1SDV_20200829T065329_20200829T065354_034118_03F64E_29F9',
'S1A_IW_GRDH_1SDV_20200829T094838_20200829T094903_034120_03F65E_0A81',
'S1A_IW_GRDH_1SDV_20200830T071607_20200830T071636_034133_03F6D5_6592',
'S1A_IW_GRDH_1SDV_20200904T224710_20200904T224739_034215_03F9B3_86D7',
'S1A_IW_GRDH_1SDV_20200905T110703_20200905T110728_034222_03F9FE_64E8',
'S1A_IW_GRDH_1SDV_20200905T214055_20200905T214120_034229_03FA38_ED1A',
'S1A_IW_GRDH_1SDV_20200907T230944_20200907T231009_034259_03FB3E_965C',
'S1A_IW_GRDH_1SDV_20200910T220117_20200910T220142_034302_03FCBE_63C9',
'S1A_IW_GRDH_1SDV_20200911T152715_20200911T152740_034313_03FD2A_FDFB',
'S1A_IW_GRDH_1SDV_20200914T155236_20200914T155301_034357_03FEB5_3FF5',
'S1A_IW_GRDH_1SDV_20200915T095403_20200915T095432_034368_03FF15_9322',
'S1A_IW_GRDH_1SDV_20200916T224804_20200916T224829_034390_03FFDA_C6FB',
'S1A_IW_GRDH_1SDV_20200918T163714_20200918T163739_034415_0400BB_0649',
'S1A_IW_GRDH_1SDV_20200918T223129_20200918T223154_034419_0400E1_96AB',
'S1A_IW_GRDH_1SDV_20200921T104144_20200921T104209_034456_04023C_C472',
'S1A_IW_GRDH_1SDV_20200924T110557_20200924T110622_034500_0403C8_453D',
'S1A_IW_GRDH_1SDV_20200927T095703_20200927T095728_034543_040541_5DFB',
'S1A_IW_GRDH_1SDV_20200927T234107_20200927T234136_034551_04058A_14EF',
'S1A_IW_GRDH_1SDV_20200929T050747_20200929T050812_034569_040632_42A5',
'S1A_IW_GRDH_1SDV_20201001T105756_20201001T105821_034602_04074E_5D47',
'S1A_IW_GRDH_1SDV_20201008T140751_20201008T140816_034706_040AEE_EAD2',
'S1A_IW_GRDH_1SDV_20201009T201613_20201009T201642_034724_040B9A_67CD',
'S1A_IW_GRDH_1SDV_20201013T213406_20201013T213431_034783_040DAD_B3DD',
'S1A_IW_GRDH_1SDV_20201014T221637_20201014T221702_034798_040E34_A2D7',
'S1A_IW_GRDH_1SDV_20201018T142910_20201018T142935_034852_04102A_7930',
'S1A_IW_GRDH_1SDV_20201021T145027_20201021T145052_034896_041191_73FA',
'S1A_IW_GRDH_1SDV_20201023T051313_20201023T051338_034919_041264_CD66',
'S1A_IW_GRDH_1SDV_20201023T051329_20201023T051354_034919_041264_545A',
'S1A_IW_GRDH_1SDV_20201024T115458_20201024T115523_034938_041306_3757',
'S1A_IW_GRDH_1SDV_20201027T044047_20201027T044112_034977_04145C_816C',
'S1A_IW_GRDH_1SDV_20201028T111704_20201028T111729_034995_041508_D5D9',
'S1A_IW_GRDH_1SDV_20201103T121212_20201103T121237_035084_041809_3140',
'S1A_IW_GRDH_1SDV_20201104T175743_20201104T175808_035102_0418B1_CFD4',
'S1A_IW_GRDH_1SDV_20201106T155808_20201106T155833_035130_04199D_4800',
'S1A_IW_GRDH_1SDV_20201107T202510_20201107T202538_035147_041A3A_6755',
'S1A_IW_GRDH_1SDV_20201109T220028_20201109T220053_035177_041B4C_E413',
'S1A_IW_GRDH_1SDV_20201110T055833_20201110T055858_035182_041B7B_5F7D',
'S1A_IW_GRDH_1SDV_20201112T222309_20201112T222338_035221_041CD2_BD9D',
'S1A_IW_GRDH_1SDV_20201114T034910_20201114T034935_035239_041D79_AA81',
'S1A_IW_GRDH_1SDV_20201116T052225_20201116T052250_035269_041E82_12BD',
'S1A_IW_GRDH_1SDV_20201116T175357_20201116T175422_035277_041EC7_0A33',
'S1A_IW_GRDH_1SDV_20201117T205332_20201117T205401_035293_041F57_5883',
'S1A_IW_GRDH_1SDV_20201124T040611_20201124T040636_035385_042277_9B55',
'S1A_IW_GRDH_1SDV_20201124T040816_20201124T040841_035385_042277_817C',
'S1A_IW_GRDH_1SDV_20201202T154342_20201202T154407_035509_0426CE_9477',
'S1A_IW_GRDH_1SDV_20201203T020715_20201203T020740_035515_0426FB_EE3B',
'S1A_IW_GRDH_1SDV_20201208T180156_20201208T180221_035598_0429D7_BBDB',
'S1A_IW_GRDH_1SDV_20201209T025943_20201209T030008_035603_042A0A_899D',
'S1A_IW_GRDH_1SDV_20201210T224256_20201210T224325_035630_042AF4_1998',
'S1A_IW_GRDH_1SDV_20201211T115403_20201211T115432_035638_042B31_BAD4',
'S1A_IW_GRDH_1SDV_20201220T095521_20201220T095546_035768_042FAC_052A',
'S1A_IW_GRDH_1SDV_20201220T234106_20201220T234134_035776_042FF9_A66B',
'S1A_IW_GRDH_1SDV_20201223T115432_20201223T115457_035813_04313B_3F2D',
'S1A_IW_GRDH_1SDV_20201224T104941_20201224T105010_035826_0431AC_AAED',
'S1A_IW_GRDH_1SDV_20201228T223602_20201228T223631_035892_043418_A489',
'S1A_IW_GRDH_1SDV_20210107T211923_20210107T211948_036037_043910_955C',
'S1A_IW_GRDH_1SDV_20210111T005559_20210111T005624_036083_043AB6_3173',
'S1A_IW_GRDH_1SDV_20210114T080140_20210114T080205_036131_043C63_3037',
'S1A_IW_GRDH_1SDV_20210114T102514_20210114T102542_036132_043C73_5FD7',
'S1A_IW_GRDH_1SDV_20210117T063647_20210117T063712_036174_043DC7_6CC8',
'S1A_IW_GRDH_1SDV_20210120T220140_20210120T220208_036227_043FC4_B850',
'S1A_IW_GRDH_1SDV_20210123T040814_20210123T040839_036260_0440DA_D0AD',
'S1A_IW_GRDH_1SDV_20210125T004125_20210125T004148_036287_0441CB_9D3B',
'S1A_IW_GRDH_1SDV_20210127T110726_20210127T110751_036322_044313_DE5C',
'S1A_IW_GRDH_1SDV_20210130T180909_20210130T180938_036371_0444B1_B3A6',
'S1A_IW_GRDH_1SDV_20210205T230221_20210205T230246_036461_0447D3_8CCD',
'S1A_IW_GRDH_1SDV_20210208T215134_20210208T215159_036504_04495A_8ACE',
'S1A_IW_GRDH_1SDV_20210213T111635_20210213T111700_036570_044BAA_87EE',
'S1A_IW_GRDH_1SDV_20210213T111815_20210213T111840_036571_044BAA_EFFF',
'S1A_IW_GRDH_1SDV_20210216T054436_20210216T054501_036611_044D0A_F4BB',
'S1A_IW_GRDH_1SDV_20210222T213607_20210222T213632_036708_04506F_C174',
'S1A_IW_GRDH_1SDV_20210225T215524_20210225T215549_036752_0451F9_2D87',
'S1A_IW_GRDH_1SDV_20210228T164718_20210228T164743_036793_04535E_6075',
'S1A_IW_GRDH_1SDV_20210302T220541_20210302T220610_036825_04547E_3617',
'S1A_IW_GRDH_1SDV_20210304T215133_20210304T215158_036854_045580_A745',
'S1A_IW_GRDH_1SDV_20210305T183416_20210305T183441_036867_0455E9_224E',
'S1A_IW_GRDH_1SDV_20210306T173830_20210306T173855_036881_045669_EB98',
'S1A_IW_GRDH_1SDV_20210308T211922_20210308T211947_036912_04578C_32C3',
'S1A_IW_GRDH_1SDV_20210316T092832_20210316T092857_037021_045B62_8A50',
'S1A_IW_GRDH_1SDV_20210318T155714_20210318T155739_037055_045C7B_7BE8',
'S1A_IW_GRDH_1SDV_20210318T213402_20210318T213427_037058_045C9B_87A6',
'S1A_IW_GRDH_1SDV_20210321T180419_20210321T180444_037100_045E1E_F51F',
'S1A_IW_GRDH_1SDV_20210405T054621_20210405T054646_037311_046559_AD20',
'S1A_IW_GRDH_1SDV_20210409T110701_20210409T110726_037372_046780_6870',
'S1A_IW_GRDH_1SDV_20210409T110726_20210409T110751_037372_046780_4FAF',
'S1A_IW_GRDH_1SDV_20210409T215314_20210409T215342_037379_0467B9_03F6',
'S1A_IW_GRDH_1SDV_20210410T023915_20210410T023940_037382_0467CD_AAE7',
'S1A_IW_GRDH_1SDV_20210410T115430_20210410T115455_037388_0467FA_6BBC',
'S1A_IW_GRDH_1SDV_20210410T223057_20210410T223126_037394_046832_0D96',
'S1A_IW_GRDH_1SDV_20210414T111636_20210414T111701_037445_046A11_081C',
'S1A_IW_GRDH_1SDV_20210419T134630_20210419T134705_037520_046C92_5C67',
'S1A_IW_GRDH_1SDV_20210422T133502_20210422T133531_037564_046E16_70CC',
'S1A_IW_GRDH_1SDV_20210422T183442_20210422T183507_037567_046E30_59A7',
'S1A_IW_GRDH_1SDV_20210425T140000_20210425T140029_037608_046F9E_855A',
'S1A_IW_GRDH_1SDV_20210505T173742_20210505T173807_037756_0474BF_7479',
'S1A_IW_GRDH_1SDV_20210506T040030_20210506T040055_037762_0474EC_7C80',
'S1A_IW_GRDH_1SDV_20210508T053003_20210508T053032_037792_0475E5_7B3F',
'S1A_IW_GRDH_1SDV_20210518T163942_20210518T164007_037945_047A75_22BA',
'S1A_IW_GRDH_1SDV_20210519T044110_20210519T044135_037952_047AB2_C27D',
'S1A_IW_GRDH_1SDV_20210523T005625_20210523T005651_038008_047C68_FE94',
'S1A_IW_GRDH_1SDV_20210523T015814_20210523T015843_038009_047C6D_19FD',
'S1A_IW_GRDH_1SDV_20210524T185906_20210524T185931_038034_047D29_07CF',
'S1A_IW_GRDH_1SDV_20210527T224256_20210527T224325_038080_047E80_D5FD',
'S1A_IW_GRDH_1SDV_20210528T115433_20210528T115458_038088_047EBE_0C02',
'S1A_IW_GRDH_1SDV_20210602T060243_20210602T060308_038157_0480DD_6BB7',
'S1A_IW_GRDH_1SDV_20210609T223219_20210609T223244_038269_048419_AA2B',
'S1A_IW_GRDH_1SDV_20210612T140003_20210612T140032_038308_048551_A942',
'S1A_IW_GRDH_1SDV_20210612T221018_20210612T221043_038313_04857A_F322',
'S1A_IW_GRDH_1SDV_20210621T115524_20210621T115549_038438_048925_3E3E',
'S1A_IW_GRDH_1SDV_20210621T183420_20210621T183445_038442_048944_E1A2',
'S1A_IW_GRDH_1SDV_20210621T231513_20210621T231538_038444_048958_CA6F',
'S1A_IW_GRDH_1SDV_20210624T154204_20210624T154229_038484_048A93_87BF',
'S1A_IW_GRDH_1SDV_20210624T154344_20210624T154409_038484_048A93_8178',
'S1A_IW_GRDH_1SDV_20210629T061906_20210629T061931_038551_048C97_235C',
'S1A_IW_GRDH_1SDV_20210703T170534_20210703T170559_038616_048E78_ACC7',
'S1A_IW_GRDH_1SDV_20210703T223221_20210703T223246_038619_048E94_2418',
'S1A_IW_GRDH_1SDV_20210703T223311_20210703T223333_038619_048E94_E29D',
'S1A_IW_GRDH_1SDV_20210704T063601_20210704T063626_038624_048EB8_87AC',
'S1A_IW_GRDH_1SDV_20210707T215504_20210707T215529_038677_049070_19B8',
'S1A_IW_GRDH_1SDV_20210708T120037_20210708T120102_038686_0490AC_2F5F',
'S1A_IW_GRDH_1SDV_20210709T142502_20210709T142527_038702_04912B_0FDD',
'S1A_IW_GRDH_1SDV_20210709T160646_20210709T160711_038703_049131_D410',
'S1A_IW_GRDH_1SDV_20210714T050536_20210714T050601_038769_049323_84FF',
'S1A_IW_GRDH_1SDV_20210714T143337_20210714T143402_038775_04934E_6462',
'S1A_IW_GRDH_1SDV_20210715T170149_20210715T170214_038791_0493BD_B473',
'S1A_IW_GRDH_1SDV_20210716T000518_20210716T000547_038795_0493DB_BE47',
'S1A_IW_GRDH_1SDV_20210717T164011_20210717T164036_038820_0494A3_1AC6',
'S1A_IW_GRDH_1SDV_20210719T111616_20210719T111641_038845_04957D_9ECD',
'S1A_IW_GRDH_1SDV_20210723T060213_20210723T060243_038901_049711_A591',
'S1A_IW_GRDH_1SDV_20210724T000136_20210724T000201_038912_04976B_207D',
'S1A_IW_GRDH_1SDV_20210726T215115_20210726T215140_038954_0498A9_7F2B',
'S1A_IW_GRDH_1SDV_20210727T223312_20210727T223334_038969_049919_B9C1',
'S1A_IW_GRDH_1SDV_20210807T215206_20210807T215231_039129_049E2F_BF10',
'S1A_IW_GRDH_1SDV_20210808T151907_20210808T151932_039140_049E7F_3948',
'S1A_IW_GRDH_1SDV_20210810T022328_20210810T022353_039161_049F40_DF05',
'S1A_IW_GRDH_1SDV_20210811T172200_20210811T172225_039185_04A00C_ED00',
'S1A_IW_GRDH_1SDV_20210813T171238_20210813T171303_039214_04A11E_2F64',
'S1A_IW_GRDH_1SDV_20210815T114323_20210815T114348_039240_04A1EF_356C',
'S1A_IW_GRDH_1SDV_20210820T115437_20210820T115502_039313_04A47C_029B',
'S1A_IW_GRDH_1SDV_20210820T223133_20210820T223158_039319_04A4B4_90DA',
'S1A_IW_GRDH_1SDV_20210821T105041_20210821T105106_039326_04A4F4_E378',
'S1A_IW_GRDH_1SDV_20210824T162335_20210824T162400_039374_04A697_FFD9',
'S1A_IW_GRDH_1SDV_20210824T164741_20210824T164811_039374_04A699_96C0',
'S1A_IW_GRDH_1SDV_20210825T223609_20210825T223638_039392_04A739_8A62',
'S1A_IW_GRDH_1SDV_20210826T050346_20210826T050411_039396_04A755_E46D',
'S1A_IW_GRDH_1SDV_20210827T235713_20210827T235742_039422_04A82D_FD06',
'S1A_IW_GRDH_1SDV_20210829T004132_20210829T004155_039437_04A8B4_8501',
'S1A_IW_GRDH_1SDV_20210829T035235_20210829T035300_039439_04A8C6_48A7',
'S1A_IW_GRDH_1SDV_20210830T224346_20210830T224415_039465_04A9AE_D4CE',
'S1A_IW_GRDH_1SDV_20210901T223134_20210901T223159_039494_04AAAE_FD16',
'S1A_IW_GRDH_1SDV_20210905T052120_20210905T052145_039542_04AC61_24FF',
'S1A_IW_GRDH_1SDV_20210906T224044_20210906T224109_039567_04AD4B_0640',
'S1A_IW_GRDH_1SDV_20210909T235958_20210910T000023_039612_04AEC5_8F4D',
'S1A_IW_GRDH_1SDV_20210911T171305_20210911T171330_039637_04AFA1_EFD9',
'S1A_IW_GRDH_1SDV_20210912T051309_20210912T051334_039644_04AFE9_8334',
'S1A_IW_GRDH_1SDV_20210918T055814_20210918T055839_039732_04B2D6_F787',
'S1A_IW_GRDH_1SDV_20210918T120341_20210918T120410_039736_04B2FB_E768',
'S1A_IW_GRDH_1SDV_20210920T114325_20210920T114350_039765_04B3EF_5595',
'S1A_IW_GRDH_1SDV_20210924T215143_20210924T215208_039829_04B625_1812',
'S1A_IW_GRDH_1SDV_20211006T015935_20211006T020001_039992_04BBD4_93CE',
'S1A_IW_GRDH_1SDV_20211006T215143_20211006T215208_040004_04BC3E_3956',
'S1A_IW_GRDH_1SDV_20211012T184434_20211012T184459_040090_04BF3B_D524',
'S1A_IW_GRDH_1SDV_20211013T064329_20211013T064354_040097_04BF7D_66FF',
'S1A_IW_GRDH_1SDV_20211016T000139_20211016T000204_040137_04C0D2_A4DA',
'S1A_IW_GRDH_1SDV_20211017T225016_20211017T225032_040165_04C1D2_87B8',
'S1A_IW_GRDH_1SDV_20211018T214913_20211018T214938_040179_04C256_EFB3',
'S1A_IW_GRDH_1SDV_20211018T215258_20211018T215323_040179_04C256_F17E',
'S1A_IW_GRDH_1SDV_20211019T041400_20211019T041425_040183_04C272_32D4',
'S1A_IW_GRDH_1SDV_20211019T223250_20211019T223315_040194_04C2D2_860E',
'S1A_IW_GRDH_1SDV_20211023T144215_20211023T144240_040248_04C4B3_FB7E',
'S1A_IW_GRDH_1SDV_20211104T094816_20211104T094845_040420_04CAB0_93B4',
'S1A_IW_GRDH_1SDV_20211104T111709_20211104T111734_040420_04CAB8_19A1',
'S1A_IW_GRDH_1SDV_20211105T075642_20211105T075707_040433_04CB2C_DAF3',
'S1A_IW_GRDH_1SDV_20211107T222614_20211107T222641_040471_04CC78_2EB1',
'S1A_IW_GRDH_1SDV_20211108T104207_20211108T104236_040478_04CCBD_A41D',
'S1A_IW_GRDH_1SDV_20211108T230157_20211108T230222_040486_04CD06_FE0F',
'S1A_IW_GRDH_1SDV_20211113T174200_20211113T174225_040556_04CF60_B384',
'S1A_IW_GRDH_1SDV_20211114T095415_20211114T095444_040565_04CFC2_CBFF',
'S1A_IW_GRDH_1SDV_20211115T061436_20211115T061501_040578_04D02E_9F76',
'S1A_IW_GRDH_1SDV_20211116T215443_20211116T215508_040602_04D10B_3DB1',
'S1A_IW_GRDH_1SDV_20211117T101655_20211117T101720_040609_04D141_EF78',
'S1A_IW_GRDH_1SDV_20211123T033720_20211123T033745_040693_04D424_DF47',
'S1A_IW_GRDH_1SDV_20211124T223314_20211124T223339_040719_04D50A_1BD4',
'S1A_IW_GRDH_1SDV_20211129T184203_20211129T184228_040790_04D78A_097C',
'S1A_IW_GRDH_1SDV_20211205T052255_20211205T052320_040869_04DA4E_A2C7',
'S1A_IW_GRDH_1SDV_20211210T094843_20211210T094908_040945_04DCDF_BD0B',
'S1A_IW_GRDH_1SDV_20211212T174642_20211212T174707_040979_04DE15_F7F9',
'S1A_IW_GRDH_1SDV_20211217T110639_20211217T110708_041047_04E05E_7C27',
'S1A_IW_GRDH_1SDV_20211219T160017_20211219T160042_041080_04E165_629A',
'S1A_IW_GRDH_1SDV_20211227T180107_20211227T180136_041198_04E546_45EF',
'S1A_IW_GRDH_1SDV_20211228T211038_20211228T211103_041214_04E5DE_A178',
'S1A_IW_GRDH_1SDV_20211229T033308_20211229T033333_041218_04E602_7217',
'S1A_IW_GRDH_1SDV_20211229T143339_20211229T143404_041225_04E643_85D7',
'S1A_IW_GRDH_1SDV_20220103T053102_20220103T053127_041292_04E884_5B4D',
'S1A_IW_GRDH_1SDV_20220103T215412_20220103T215441_041302_04E8DF_43A1',
'S1A_IW_GRDH_1SDV_20220104T055837_20220104T055902_041307_04E90C_7678',
'S1A_IW_GRDH_1SDV_20220106T164750_20220106T164815_041343_04EA3B_25DD',
'S1A_IW_GRDH_1SDV_20220107T154739_20220107T154804_041357_04EABB_17BD',
'S1A_IW_GRDH_1SDV_20220112T213459_20220112T213524_041433_04ED39_0AA3',
'S1A_IW_GRDH_1SDV_20220113T004913_20220113T004939_041435_04ED42_B850',
'S1A_IW_GRDH_1SDV_20220120T180106_20220120T180135_041548_04F0F2_ABC1',
'S1A_IW_GRDH_1SDV_20220121T184233_20220121T184258_041563_04F17A_4C48',
'S1A_IW_GRDH_1SDV_20220122T192554_20220122T192619_041578_04F203_6296',
'S1A_IW_GRDH_1SDV_20220123T054937_20220123T055002_041584_04F239_03A5',
'S1A_IW_GRDH_1SDV_20220125T180944_20220125T181009_041621_04F377_2CAC',
'S1A_IW_GRDH_1SDV_20220127T111731_20220127T111756_041645_04F44B_7C58',
'S1A_IW_GRDH_1SDV_20220129T142912_20220129T142937_041677_04F566_8302',
'S1A_IW_GRDH_1SDV_20220203T215229_20220203T215254_041754_04F7FF_8566',
'S1A_IW_GRDH_1SDV_20220204T115125_20220204T115153_041763_04F84B_A71D',
'S1A_IW_GRDH_1SDV_20220206T221429_20220206T221458_041798_04F994_0D42',
'S1A_IW_GRDH_1SDV_20220210T213919_20220210T213944_041856_04FBA1_97D7',
'S1A_IW_GRDH_1SDV_20220212T185815_20220212T185844_041884_04FC8B_9072',
'S1A_IW_GRDH_1SDV_20220227T052317_20220227T052345_042094_0503D4_B25A',
'S1A_IW_GRDH_1SDV_20220301T173811_20220301T173836_042131_05050E_9020',
'S1A_IW_GRDH_1SDV_20220301T174131_20220301T174156_042131_05050E_0385',
'S1A_IW_GRDH_1SDV_20220310T102558_20220310T102627_042257_050959_AA40',
'S1A_IW_GRDH_1SDV_20220312T223246_20220312T223311_042294_050A9E_98D6',
'S1A_IW_GRDH_1SDV_20220313T105039_20220313T105104_042301_050ADB_346D',
'S1A_IW_GRDH_1SDV_20220315T225510_20220315T225539_042338_050C1E_DB6C',
'S1A_IW_GRDH_1SDV_20220316T111755_20220316T111820_042346_050C69_F6FC',
'S1A_IW_GRDH_1SDV_20220321T021516_20220321T021541_042413_050EA7_AAF1',
'S1A_IW_GRDH_1SDV_20220324T223246_20220324T223311_042469_051092_7CA7',
'S1A_IW_GRDH_1SDV_20220329T223951_20220329T224016_042542_05131C_EEF5',
'S1A_IW_GRDH_1SDV_20220407T221729_20220407T221754_042673_05176E_6CDE',
'S1A_IW_GRDH_1SDV_20220411T051423_20220411T051448_042721_05191B_F532',
'S1A_IW_GRDH_1SDV_20220411T064531_20220411T064556_042722_051926_F445',
'S1A_IW_GRDH_1SDV_20220414T220847_20220414T220914_042775_051AE6_9848',
'S1A_IW_GRDH_1SDV_20220415T224742_20220415T224807_042790_051B6C_6294',
'S1A_IW_GRDH_1SDV_20220417T223222_20220417T223247_042819_051C5F_4DFD',
'S1A_IW_GRDH_1SDV_20220418T045058_20220418T045123_042823_051C71_27F9',
'S1A_IW_GRDH_1SDV_20220421T003324_20220421T003347_042864_051DDB_8D40',
'S1A_IW_GRDH_1SDV_20220425T013442_20220425T013505_042923_051FB9_1A20',
'S1A_IW_GRDH_1SDV_20220429T023921_20220429T023946_042982_0521AE_EE33',
'S1A_IW_GRDH_1SDV_20220429T115502_20220429T115527_042988_0521E1_1C93',
'S1A_IW_GRDH_1SDV_20220429T223222_20220429T223247_042994_05221A_8844',
'S1A_IW_GRDH_1SDV_20220502T140006_20220502T140035_043033_052362_A802',
'S1A_IW_GRDH_1SDV_20220506T222522_20220506T222547_043096_052579_798F',
'S1A_IW_GRDH_1SDV_20220508T181447_20220508T181512_043123_052660_4852',
'S1A_IW_GRDH_1SDV_20220510T143634_20220510T143659_043150_052746_87D4',
'S1A_IW_GRDH_1SDV_20220517T160533_20220517T160558_043253_052A63_A207',
'S1A_IW_GRDH_1SDV_20220522T051218_20220522T051243_043319_052C50_6980',
'S1A_IW_GRDH_1SDV_20220525T113434_20220525T113459_043367_052DC0_86AC',
'S1A_IW_GRDH_1SDV_20220527T180657_20220527T180722_043400_052EC7_10F1',
'S1A_IW_GRDH_1SDV_20220528T101655_20220528T101720_043409_052F01_B920',
'S1A_IW_GRDH_1SDV_20220531T130719_20220531T130746_043455_05305A_98F6',
'S1A_IW_GRDH_1SDV_20220603T175428_20220603T175453_043502_0531AE_D192',
'S1A_IW_GRDH_1SDV_20220605T092352_20220605T092417_043526_053269_8402',
'S1A_IW_GRDH_1SDV_20220611T182854_20220611T182919_043619_05352D_0483',
'S1A_IW_GRDH_1SDV_20220614T043327_20220614T043352_043654_053638_0124',
'S1A_IW_GRDH_1SDV_20220615T143341_20220615T143406_043675_0536E0_6C3C',
'S1A_IW_GRDH_1SDV_20220621T170356_20220621T170421_043764_053992_DFCE',
'S1A_IW_GRDH_1SDV_20220622T001714_20220622T001749_043768_0539B3_29B1',
'S1A_IW_GRDH_1SDV_20220624T031513_20220624T031538_043799_053A90_D6DF',
'S1A_IW_GRDH_1SDV_20220626T224718_20220626T224747_043840_053BD3_7F88',
'S1A_IW_GRDH_1SDV_20220629T105713_20220629T105738_043877_053CEB_4888',
'S1A_IW_GRDH_1SDV_20220704T224226_20220704T224251_043957_053F4C_1D1D',
'S1B_IW_GRDH_1SDV_20200725T064830_20200725T064855_022624_02AEFC_0C98',
'S1B_IW_GRDH_1SDV_20200727T092212_20200727T092243_022655_02AFFE_98C7',
'S1B_IW_GRDH_1SDV_20200731T055735_20200731T055800_022711_02B1B2_854D',
'S1B_IW_GRDH_1SDV_20200803T062422_20200803T062447_022755_02B2F6_27F0',
'S1B_IW_GRDH_1SDV_20200803T173558_20200803T173623_022762_02B332_E4F2',
'S1B_IW_GRDH_1SDV_20200803T173623_20200803T173648_022762_02B332_AA8F',
'S1B_IW_GRDH_1SDV_20200806T162023_20200806T162048_022805_02B484_06D3',
'S1B_IW_GRDH_1SDV_20200807T025207_20200807T025232_022811_02B4B5_9FCE',
'S1B_IW_GRDH_1SDV_20200810T172848_20200810T172904_022864_02B66F_595C',
'S1B_IW_GRDH_1SDV_20200812T170559_20200812T170624_022893_02B744_B462',
'S1B_IW_GRDH_1SDV_20200819T054843_20200819T054908_022988_02BA34_E26A',
'S1B_IW_GRDH_1SDV_20200820T092213_20200820T092238_023005_02BABB_74F3',
'S1B_IW_GRDH_1SDV_20200820T174056_20200820T174132_023010_02BAE9_C64D',
'S1B_IW_GRDH_1SDV_20200823T094743_20200823T094812_023049_02BC37_604E',
'S1B_IW_GRDH_1SDV_20200826T164820_20200826T164845_023097_02BDAD_5CDF',
'S1B_IW_GRDH_1SDV_20200827T061833_20200827T061858_023105_02BDDE_2D7D',
'S1B_IW_GRDH_1SDV_20200830T080240_20200830T080309_023150_02BF42_95BC',
'S1B_IW_GRDH_1SDV_20200901T074652_20200901T074721_023179_02C030_41EF',
'S1B_IW_GRDH_1SDV_20200901T174057_20200901T174130_023185_02C064_C8EC',
'S1B_IW_GRDH_1SDV_20200906T175227_20200906T175252_023258_02C2A8_872C',
'S1B_IW_GRDH_1SDV_20200908T172846_20200908T172911_023287_02C398_079F',
'S1B_IW_GRDH_1SDV_20200909T034850_20200909T034915_023293_02C3CD_F327',
'S1B_IW_GRDH_1SDV_20200909T050510_20200909T050539_023294_02C3D3_5BFE',
'S1B_IW_GRDH_1SDV_20200909T181350_20200909T181415_023302_02C415_31CE',
'S1B_IW_GRDH_1SDV_20200916T045603_20200916T045633_023396_02C70E_91BE',
'S1B_IW_GRDH_1SDV_20200918T045856_20200918T045921_023425_02C7F1_107A',
'S1B_IW_GRDH_1SDV_20200918T214237_20200918T214302_023435_02C842_FBF3',
'S1B_IW_GRDH_1SDV_20200919T054009_20200919T054034_023440_02C861_C12B',
'S1B_IW_GRDH_1SDV_20200919T054012_20200919T054037_023440_02C861_9310',
'S1B_IW_GRDH_1SDV_20200919T054419_20200919T054444_023440_02C861_A430',
'S1B_IW_GRDH_1SDV_20200920T173001_20200920T173026_023462_02C912_6B7E',
'S1B_IW_GRDH_1SDV_20200930T045856_20200930T045921_023600_02CD6C_6A16',
'S1B_IW_GRDH_1SDV_20201001T053714_20201001T053739_023615_02CDE1_C3BD',
'S1B_IW_GRDH_1SDV_20201001T164706_20201001T164731_023622_02CE1A_5A7E',
'S1B_IW_GRDH_1SDV_20201002T172847_20201002T172912_023637_02CE8C_FC31',
'S1B_IW_GRDH_1SDV_20201005T064329_20201005T064358_023674_02CFB0_F7B2',
'S1B_IW_GRDH_1SDV_20201006T020955_20201006T021020_023686_02D017_0A1B',
'S1B_IW_GRDH_1SDV_20201006T115402_20201006T115425_023692_02D04C_FE86',
'S1B_IW_GRDH_1SDV_20201015T050510_20201015T050539_023819_02D437_8BEC',
'S1B_IW_GRDH_1SDV_20201018T183430_20201018T183455_023871_02D5CE_E8CD',
'S1B_IW_GRDH_1SDV_20201021T172050_20201021T172115_023914_02D741_0719',
'S1B_IW_GRDH_1SDV_20201022T033938_20201022T034003_023920_02D76C_177F',
'S1B_IW_GRDH_1SDV_20201023T170409_20201023T170433_023943_02D81C_C8C1',
'S1B_IW_GRDH_1SDV_20201025T164846_20201025T164911_023972_02D908_B5CD',
'S1B_IW_GRDH_1SDV_20201103T081047_20201103T081116_024098_02DCED_4307',
'S1B_IW_GRDH_1SDV_20201106T181706_20201106T181731_024148_02DE74_A73C',
'S1B_IW_GRDH_1SDV_20201107T173257_20201107T173322_024162_02DEE1_CE9F',
'S1B_IW_GRDH_1SDV_20201108T034825_20201108T034850_024168_02DF16_86C2',
'S1B_IW_GRDH_1SDV_20201109T102532_20201109T102557_024186_02DFA7_FD74',
'S1B_IW_GRDH_1SDV_20201112T045635_20201112T045700_024227_02E0E5_11BA',
'S1B_IW_GRDH_1SDV_20201112T105616_20201112T105635_024231_02E10A_8A74',
'S1B_IW_GRDH_1SDV_20201114T013822_20201114T013850_024254_02E1E1_4638',
'S1B_IW_GRDH_1SDV_20201115T180127_20201115T180152_024279_02E2A2_784D',
'S1B_IW_GRDH_1SDV_20201119T154826_20201119T154851_024336_02E455_8DB6',
'S1B_IW_GRDH_1SDV_20201123T115402_20201123T115425_024392_02E625_5AFA',
'S1B_IW_GRDH_1SDV_20201203T080555_20201203T080623_024535_02EAA8_1CB3',
'S1B_IW_GRDH_1SDV_20201212T100558_20201212T100623_024668_02EEFE_89B4',
'S1B_IW_GRDH_1SDV_20201213T082213_20201213T082238_024681_02EF68_CBA2',
'S1B_IW_GRDH_1SDV_20201229T054850_20201229T054915_024913_02F6E8_4CB9',
'S1B_IW_GRDH_1SDV_20201230T032144_20201230T032213_024926_02F74E_477A',
'S1B_IW_GRDH_1SDV_20210103T151943_20210103T152008_024992_02F972_74D5',
'S1B_IW_GRDH_1SDV_20210108T060259_20210108T060324_025059_02FB91_5DF2',
'S1B_IW_GRDH_1SDV_20210108T152831_20210108T152900_025065_02FBC1_5A3D',
'S1B_IW_GRDH_1SDV_20210109T051051_20210109T051116_025073_02FC03_DDA5',
'S1B_IW_GRDH_1SDV_20210116T174610_20210116T174635_025183_02FF94_7B48',
'S1B_IW_GRDH_1SDV_20210207T034025_20210207T034050_025495_03099F_537F',
'S1B_IW_GRDH_1SDV_20210210T040747_20210210T040812_025539_030B0E_215C',
'S1B_IW_GRDH_1SDV_20210221T214324_20210221T214349_025710_0310A1_708C',
'S1B_IW_GRDH_1SDV_20210330T182527_20210330T182552_026248_0321F8_623A',
'S1B_IW_GRDH_1SDV_20210403T161317_20210403T161342_026305_0323BE_A751',
'S1B_IW_GRDH_1SDV_20210407T154109_20210407T154134_026363_0325A8_F678',
'S1B_IW_GRDH_1SDV_20210410T050251_20210410T050316_026400_0326C5_08FD',
'S1B_IW_GRDH_1SDV_20210410T174454_20210410T174519_026408_032703_72DA',
'S1B_IW_GRDH_1SDV_20210412T073925_20210412T073959_026431_0327BC_3F90',
'S1B_IW_GRDH_1SDV_20210412T154848_20210412T154913_026436_0327E9_9B9B',
'S1B_IW_GRDH_1SDV_20210413T070653_20210413T070718_026445_032837_1AA4',
'S1B_IW_GRDH_1SDV_20210426T102555_20210426T102620_026636_032E57_2730',
'S1B_IW_GRDH_1SDV_20210430T180919_20210430T180944_026700_033073_D411',
'S1B_IW_GRDH_1SDV_20210506T173255_20210506T173320_026787_033328_F6E4',
'S1B_IW_GRDH_1SDV_20210507T052900_20210507T052929_026794_033366_C7A0',
'S1B_IW_GRDH_1SDV_20210524T180856_20210524T180921_027050_033B56_7398',
'S1B_IW_GRDH_1SDV_20210529T100234_20210529T100259_027117_033D47_7A3C',
'S1B_IW_GRDH_1SDV_20210604T045512_20210604T045537_027202_033FD0_5BD8',
'S1B_IW_GRDH_1SDV_20210613T233207_20210613T233232_027344_03440C_B11F',
'S1B_IW_GRDH_1SDV_20210615T151813_20210615T151838_027369_0344D0_5F31',
'S1B_IW_GRDH_1SDV_20210615T165449_20210615T165514_027370_0344D7_8940',
'S1B_IW_GRDH_1SDV_20210623T173003_20210623T173028_027487_034821_2CDD',
'S1B_IW_GRDH_1SDV_20210626T051247_20210626T051312_027523_034911_53D5',
'S1B_IW_GRDH_1SDV_20210628T155657_20210628T155726_027559_034A2F_9F11',
'S1B_IW_GRDH_1SDV_20210704T164823_20210704T164848_027647_034CBC_1AE3',
'S1B_IW_GRDH_1SDV_20210705T044729_20210705T044754_027654_034CE9_B778',
'S1B_IW_GRDH_1SDV_20210707T172027_20210707T172050_027691_034E02_CA71',
'S1B_IW_GRDH_1SDV_20210708T175651_20210708T175716_027706_034E71_C399',
'S1B_IW_GRDH_1SDV_20210715T160447_20210715T160512_027807_035175_C66C',
'S1B_IW_GRDH_1SDV_20210719T040729_20210719T040758_027858_0352F9_CF72',
'S1B_IW_GRDH_1SDV_20210721T055232_20210721T055257_027888_0353E2_9B6E',
'S1B_IW_GRDH_1SDV_20210722T032148_20210722T032217_027901_03544C_A13B',
'S1B_IW_GRDH_1SDV_20210723T164019_20210723T164044_027924_0354FB_5BDF',
'S1B_IW_GRDH_1SDV_20210808T050454_20210808T050519_028150_035BB4_BD15',
'S1B_IW_GRDH_1SDV_20210812T172004_20210812T172029_028216_035DB8_40B0',
'S1B_IW_GRDH_1SDV_20210813T051225_20210813T051250_028223_035DEA_1B82',
'S1B_IW_GRDH_1SDV_20210815T155934_20210815T155959_028259_035F0E_3CDF',
'S1B_IW_GRDH_1SDV_20210818T034327_20210818T034352_028295_03603E_60D0',
'S1B_IW_GRDH_1SDV_20210822T173646_20210822T173719_028362_036253_DFB1',
'S1B_IW_GRDH_1SDV_20210823T163024_20210823T163049_028376_0362C7_1AA8',
'S1B_IW_GRDH_1SDV_20210824T102537_20210824T102602_028386_036318_EECA',
'S1B_IW_GRDH_1SDV_20210826T100855_20210826T100924_028415_0363F1_D947',
'S1B_IW_GRDH_1SDV_20210827T173649_20210827T173718_028435_03648C_EC90',
'S1B_IW_GRDH_1SDV_20210901T075455_20210901T075531_028502_0366AC_3490',
'S1B_IW_GRDH_1SDV_20210905T171300_20210905T171325_028566_0368AE_8544',
'S1B_IW_GRDH_1SDV_20210908T174128_20210908T174153_028610_036A08_3771',
'S1B_IW_GRDH_1SDV_20210908T174423_20210908T174448_028610_036A08_28AF',
'S1B_IW_GRDH_1SDV_20210910T152750_20210910T152819_028637_036AF4_F002',
'S1B_IW_GRDH_1SDV_20210921T035936_20210921T040001_028791_036F9B_E867',
'S1B_IW_GRDH_1SDV_20211007T142440_20211007T142505_029031_0376D4_443A',
'S1B_IW_GRDH_1SDV_20211109T153808_20211109T153837_029513_0385B9_D242',
'S1B_IW_GRDH_1SDV_20211114T044823_20211114T044848_029579_0387AA_0118',
'S1B_IW_GRDH_1SDV_20211119T174104_20211119T174129_029660_038A34_DD17',
'S1B_IW_GRDH_1SDV_20211121T142946_20211121T143011_029687_038B19_894D',
'S1B_IW_GRDH_1SDV_20211206T032413_20211206T032438_029899_0391BA_B81D',
'S1B_IW_GRDH_1SDV_20211207T100514_20211207T100539_029918_03924D_CD13',
'S1B_IW_GRDH_1SDV_20211215T061642_20211215T061707_030032_0395FF_3803',
'S1B_IW_GRDH_1SDV_20211217T184328_20211217T184353_030069_03971B_DC1A',
'S1B_IW_GRDH_1SDV_20211222T061035_20211222T061100_030134_039922_69EB',
'S1B_IW_GRDH_1SSV_20200801T105756_20200801T105824_022728_02B236_F52F',
'S1B_IW_GRDH_1SSV_20210116T105755_20210116T105824_025178_02FF7A_F2F3',
'S1B_IW_GRDH_1SSV_20210317T105754_20210317T105823_026053_031BD4_B5B5',
'S1B_IW_GRDH_1SSV_20211112T105804_20211112T105832_029553_0386F3_BBD7'
	)
	GROUP BY slick__id, grd__starttime, grd__pid
) as sv
ON grd.starttime > sv.grd__starttime 
AND grd.starttime < (INTERVAL '24 hours'+sv.grd__starttime)
AND ST_Intersects(sv.poly, grd.geometry)
