# pipeline_config.py

# 城市代碼
CITIES = {"001": "台北市"}

# 行政區代碼 (ZipCode)
ZIP_CODES = {
    "111": "士林區", "103": "大同區", "106": "大安區", "104": "中山區",
    "100": "中正區", "114": "內湖區", "116": "文山區", "112": "北投區",
    "105": "松山區", "110": "信義區", "115": "南港區", "108": "萬華區"
}

# 行業別代碼表
INDUSTRY_CODES = {
    "0017": "旅行業", "0009": "旅宿業", "0007": "交通運輸業", "0018": "觀光遊樂業",
    "0008": "其他業別-餐飲", "0020": "其他業別-農特產及手工藝品", "0021": "其他業別-加油站",
    "0015": "其他業別-體育用品", "0016": "其他業別-其他服務", "0025": "其他業別-服飾",
    "0026": "其他業別-皮鞋皮件", "0027": "其他業別-美容護膚", "0023": "其他業別-商圈及其他",
    "0030": "其他業別-藝文圖書", "0031": "其他業別"
}

# 同義詞對照表 (用於隱藏標籤)
SYNONYMS_MAP = {
    # === [服飾 / 運動 / 鞋類] ===
    "uniqlo": "優衣庫 UQ 台灣優衣庫 衣服",
    "zara": "莎拉 薩拉 荷蘭商颯拉",
    "h&m": "hm hnm 慕尚展現",
    "net": "net服飾 主富 主富服裝",
    "gap": "蓋璞",
    "gu": "極優",
    "adidas": "愛迪達 三葉草 台灣阿迪達斯",
    "nike": "耐吉 勾勾 必爾斯藍基",
    "puma": "彪馬 台灣彪馬",
    "newbalance": "nb 紐巴倫 台灣紐巴倫",
    "skechers": "思凱捷 思克威爾",
    "underarmour": "ua 安德瑪",
    "levi": "levis 李維斯 麗威森 牛仔褲",
    "roots": "加拿大海狸",
    "timberland": "踢不爛 天伯倫",
    "birkenstock": "勃肯",
    "crocs": "布希鞋 鱷魚鞋",
    "porter": "波特 波特包 尚立",
    "lululemon": "露露檸檬 露露樂蒙",
    "la_new": "la new 老牛皮 老牛皮國際",
    "aso": "阿瘦 阿瘦皮鞋 阿瘦實業",
    "giordano": "佐丹奴 澳洲商佐丹奴",
    "momentum": "摩曼頓",

    # === [餐飲 / 速食 / 連鎖] ===
    "mcdonalds": "麥當勞 mcd 麥當當 和德昌",
    "kfc": "肯德基 炸雞",
    "starbucks": "星巴克 悠旅生活",
    "burgerking": "漢堡王",
    "mos": "摩斯 摩斯漢堡",
    "subway": "賽百味 潛艇堡",
    "pizzahut": "必勝客",
    "dominos": "達美樂 台灣達美樂",
    "sushiro": "壽司郎 台灣壽司郎",
    "kurasushi": "藏壽司 亞洲藏壽司",
    "hama": "HAMA壽司 哈瑪壽司",
    "saizeriya": "薩莉亞 台灣薩莉亞",
    "coco": "都可",
    "50lan": "50嵐 五十嵐",
    "chunshuitang": "春水堂",
    "louisa": "路易莎",
    "cama": "卡瑪 咖碼",
    "85c": "85度c",
    "wowprime": "王品 王品餐飲 王品集團",
    "tasty": "西堤",
    "tau": "陶板屋",
    "giguo": "聚 北海道昆布鍋",
    "12hotpot": "石二鍋",
    "haidilao": "海底撈",
    "ding_tai_fung": "鼎泰豐",

    # === [零售 / 藥妝 / 百貨 / 超市] ===
    "7-eleven": "7-11 統一超商 小七 seven",
    "family": "全家 全家便利商店",
    "hilife": "萊爾富",
    "okmart": "ok超商",
    "pxmart": "全聯 福利中心 全聯實業",
    "carrefour": "家樂福 家福",
    "costco": "好市多",
    "rt-mart": "大潤發",
    "watsons": "屈臣氏 台灣屈臣氏",
    "cosmed": "康是美",
    "poya": "寶雅",
    "ikea": "宜家 宜家家居",
    "nitori": "宜得利",
    "muji": "無印良品",
    "decathlon": "迪卡儂",
    "hola": "特力和樂",
    "bnq": "特力屋",
    "eslite": "誠品 誠品書店",
    "sogo": "遠東sogo 崇光百貨",
    "mitsukoshi": "新光三越",
    "breeze": "微風 微風廣場",
    "qsquare": "京站",

    # === [3C / 電信 / 家電] ===
    "apple": "蘋果",
    "samsung": "三星",
    "asus": "華碩",
    "acer": "宏碁",
    "sony": "索尼",
    "dyson": "戴森",
    "xiaomi": "小米",
    "cht": "中華電信",
    "fet": "遠傳",
    "twm": "台灣大哥大",
    "tsannkuen": "燦坤",
    "elifemall": "全國電子",
    "senao": "神腦",

    # === [交通 / 旅遊 / 汽車] ===
    "ubike": "微笑單車",
    "gogoro": "Gogoro",
    "klook": "客路",
    "kkday": "酷遊",
    "liontravel": "雄獅",
    "colatour": "可樂旅遊",
    "toyota": "豐田",
    "nissan": "裕隆",
    "giant": "捷安特",
    "merida": "美利達",
}

# 價格等級級距 (Price Levels)
# 格式: "行業別名稱": [Level1上限, Level2上限, Level3上限, Level4上限]
# 範例: [1500, 3000, 5000, 8000] 代表:
#   Level 1: < 1500
#   Level 2: 1500 - 3000
#   Level 3: 3000 - 5000
#   Level 4: 5000 - 8000
#   Level 5: > 8000
PRICE_THRESHOLDS = {
    "旅宿業": [1500, 3000, 5000, 8000],
    "default": [200, 500, 1000, 2000] 
}
