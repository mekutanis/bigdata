"""
End-to-end batch pipeline:
  1. Kafka -> Bronze/Silver/Gold (Delta Lake)
  2. EDA istatistikleri
  3. Feature Engineering (TF-IDF + 5 sayısal feature)
  4. 5 ML model + MLflow log
Tüm sonuçlar /workspace/_results.json'a yazılır.
"""
import json
import time
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType, TimestampType, DoubleType
)
from pyspark.sql.functions import from_json, col, length, trim, when, to_date, count
from pyspark.sql.functions import sum as spark_sum

results = {}

# ──────────────────────────────────────────────────────────────────────
# 1) SPARK SESSION
# ──────────────────────────────────────────────────────────────────────
spark = (
    SparkSession.builder
    .appName("SteamReviews_BatchPipeline")
    .master("spark://spark-master:7077")
    .config("spark.jars.packages",
            "io.delta:delta-core_2.12:2.4.0,"
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.driver.memory", "4g")
    .config("spark.executor.memory", "2g")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")
print(f"[OK] Spark {spark.version} on {spark.sparkContext.master}")

# ──────────────────────────────────────────────────────────────────────
# 2) KAFKA -> BRONZE
# ──────────────────────────────────────────────────────────────────────
schema = StructType([
    StructField("timestamp", TimestampType(), True),
    StructField("user_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("app_id", IntegerType(), True),
    StructField("app_name", StringType(), True),
    StructField("review_text", StringType(), True),
    StructField("review_score", IntegerType(), True),
    StructField("review_votes", IntegerType(), True),
])

print("[..] Kafka'dan batch okuma...")
raw_df = (
    spark.read.format("kafka")
    .option("kafka.bootstrap.servers", "kafka:29092")
    .option("subscribe", "steam-reviews")
    .option("startingOffsets", "earliest")
    .option("endingOffsets", "latest")
    .load()
)

parsed_df = (
    raw_df.selectExpr("CAST(value AS STRING) as json_str", "timestamp as kafka_ts")
    .select(from_json(col("json_str"), schema).alias("d"), col("kafka_ts"))
    .select("d.*", "kafka_ts")
)
bronze_count = parsed_df.count()
print(f"[OK] Kafka'dan {bronze_count:,} mesaj okundu (Bronze)")

# BRONZE yazma (overwrite — temiz başlangıç)
(parsed_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
 .save("/delta/bronze/steam_reviews"))
print("[OK] Bronze yazıldı")

# ──────────────────────────────────────────────────────────────────────
# 3) BRONZE -> SILVER (temizleme)
# ──────────────────────────────────────────────────────────────────────
cleaned_df = (
    parsed_df
    .filter(col("review_text").isNotNull())
    .filter(length(trim(col("review_text"))) >= 10)
    .filter(col("review_score").isin(1, -1))
    .dropDuplicates(["app_id", "review_text"])
    .withColumn("label", when(col("review_score") == 1, 1).otherwise(0))
)
silver_df = cleaned_df.select(
    "timestamp", "user_id", "event_type", "app_id", "app_name",
    "review_text", "review_score", "review_votes", "label"
)
silver_df.cache()
silver_count = silver_df.count()
print(f"[OK] Silver: {silver_count:,} kayıt (temizleme sonrası)")

(silver_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
 .save("/delta/silver/steam_reviews"))
print("[OK] Silver yazıldı")

# ──────────────────────────────────────────────────────────────────────
# 4) SILVER -> GOLD (günlük istatistikler)
# ──────────────────────────────────────────────────────────────────────
gold_daily = (
    silver_df
    .withColumn("review_date", to_date(col("timestamp")))
    .groupBy("review_date", "app_id", "app_name")
    .agg(
        count("*").alias("total_reviews"),
        spark_sum(when(col("review_score") == 1, 1).otherwise(0)).alias("positive_count"),
        spark_sum(when(col("review_score") == -1, 1).otherwise(0)).alias("negative_count"),
    )
)
(gold_daily.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
 .save("/delta/gold/daily_stats"))
gold_daily_count = gold_daily.count()
print(f"[OK] Gold daily_stats: {gold_daily_count:,} satır")

# ──────────────────────────────────────────────────────────────────────
# 5) EDA İSTATİSTİKLERİ
# ──────────────────────────────────────────────────────────────────────
print("[..] EDA istatistikleri...")
toplam_kayit = silver_count
benzersiz_oyun = silver_df.select("app_name").distinct().count()
pos_count = silver_df.filter(col("label") == 1).count()
neg_count = silver_df.filter(col("label") == 0).count()
pos_oran = pos_count / toplam_kayit * 100
neg_oran = neg_count / toplam_kayit * 100

avg_len_row = silver_df.select(F.avg(F.length("review_text")).alias("a")).collect()[0]
avg_text_len = float(avg_len_row["a"])

top_game_row = (silver_df.groupBy("app_name").count()
                .orderBy(F.desc("count")).limit(1).collect()[0])
top_game = top_game_row["app_name"]
top_game_count = int(top_game_row["count"])

# Sınıf bazında ortalama metin uzunluğu
len_by_class = (silver_df.groupBy("label")
                .agg(F.avg(F.length("review_text")).alias("avg_len"))
                .collect())
len_by_class_map = {int(r["label"]): float(r["avg_len"]) for r in len_by_class}

# review_votes istatistikleri
votes_stats = silver_df.select(
    F.avg("review_votes").alias("mean"),
    F.expr("percentile_approx(review_votes, 0.5)").alias("median"),
    F.max("review_votes").alias("max"),
).collect()[0]

eda = {
    "toplam_kayit": int(toplam_kayit),
    "bronze_kayit": int(bronze_count),
    "benzersiz_oyun": int(benzersiz_oyun),
    "pozitif_sayi": int(pos_count),
    "negatif_sayi": int(neg_count),
    "pozitif_oran": round(pos_oran, 2),
    "negatif_oran": round(neg_oran, 2),
    "ortalama_metin_uzunlugu": round(avg_text_len, 1),
    "en_cok_yorumlanan_oyun": top_game,
    "en_cok_yorumlanan_oyun_sayisi": top_game_count,
    "ortalama_uzunluk_pozitif": round(len_by_class_map.get(1, 0.0), 1),
    "ortalama_uzunluk_negatif": round(len_by_class_map.get(0, 0.0), 1),
    "votes_mean": round(float(votes_stats["mean"]), 2),
    "votes_median": int(votes_stats["median"] or 0),
    "votes_max": int(votes_stats["max"] or 0),
    "siniflar_orani": round(max(pos_count, neg_count) / max(min(pos_count, neg_count), 1), 2),
    "gold_daily_count": int(gold_daily_count),
}
results["eda"] = eda
print(f"[OK] EDA: {eda}")

# ──────────────────────────────────────────────────────────────────────
# 6) FEATURE ENGINEERING
# ──────────────────────────────────────────────────────────────────────
print("[..] Feature Engineering...")
df_numeric = (
    silver_df
    .withColumn("text_length", F.length(F.col("review_text")).cast(DoubleType()))
    .withColumn("word_count", F.size(F.split(F.col("review_text"), r"\s+")).cast(DoubleType()))
    .withColumn("review_votes_d", F.col("review_votes").cast(DoubleType()))
    .withColumn("upper_chars",
                F.length(F.regexp_replace(F.col("review_text"), r"[^A-Z]", "")).cast(DoubleType()))
    .withColumn("uppercase_ratio",
                F.when(F.col("text_length") > 0, F.col("upper_chars") / F.col("text_length"))
                 .otherwise(F.lit(0.0)))
    .drop("upper_chars")
    .withColumn("exclamation_count",
                F.length(F.regexp_replace(F.col("review_text"), r"[^!]", "")).cast(DoubleType()))
)

from pyspark.ml.feature import Tokenizer, StopWordsRemover, HashingTF, IDF, VectorAssembler
from pyspark.ml import Pipeline

tokenizer = Tokenizer(inputCol="review_text", outputCol="tokens_raw")
stop_remover = StopWordsRemover(inputCol="tokens_raw", outputCol="tokens")
hashing_tf = HashingTF(inputCol="tokens", outputCol="tf_features", numFeatures=5000)
idf = IDF(inputCol="tf_features", outputCol="tfidf_features", minDocFreq=5)
assembler = VectorAssembler(
    inputCols=["tfidf_features", "text_length", "word_count",
               "review_votes_d", "uppercase_ratio", "exclamation_count"],
    outputCol="features", handleInvalid="skip",
)
fe_pipeline = Pipeline(stages=[tokenizer, stop_remover, hashing_tf, idf, assembler])
fe_model = fe_pipeline.fit(df_numeric)
df_features = fe_model.transform(df_numeric)

df_features_final = df_features.select(
    "app_id", "app_name", "user_id", "timestamp", "label",
    "text_length", "word_count", "review_votes_d",
    "uppercase_ratio", "exclamation_count", "features",
)

# class weight
label_counts = df_features_final.groupBy("label").count().collect()
total = sum(r["count"] for r in label_counts)
n_classes = len(label_counts)
weights = {r["label"]: total / (n_classes * r["count"]) for r in label_counts}
weight_expr = F.when(F.col("label") == 1, F.lit(weights.get(1, 1.0))).otherwise(F.lit(weights.get(0, 1.0)))
df_with_weights = df_features_final.withColumn("classWeight", weight_expr)

(df_with_weights.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
 .save("/delta/gold/features_table"))
print(f"[OK] Features table yazıldı: {df_with_weights.count():,} satır")
print(f"[OK] Class weights: {weights}")
results["features"] = {
    "n_features": 5005,
    "tfidf_dim": 5000,
    "numeric_features": ["text_length", "word_count", "review_votes_d",
                         "uppercase_ratio", "exclamation_count"],
    "class_weight_0": round(weights.get(0, 1.0), 4),
    "class_weight_1": round(weights.get(1, 1.0), 4),
}

# ──────────────────────────────────────────────────────────────────────
# 7) ML MODELS + MLFLOW
# ──────────────────────────────────────────────────────────────────────
print("[..] ML Modelleri eğitiliyor...")
from pyspark.ml.classification import (
    LogisticRegression, DecisionTreeClassifier, RandomForestClassifier,
    GBTClassifier, NaiveBayes
)
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator

import mlflow
import mlflow.spark
mlflow.set_tracking_uri("http://mlflow:5000")
mlflow.set_experiment("steam_reviews_classification")

features_df = spark.read.format("delta").load("/delta/gold/features_table")
train, test = features_df.randomSplit([0.8, 0.2], seed=42)
train.cache()
test.cache()
train_n, test_n = train.count(), test.count()
print(f"[OK] Train: {train_n:,} | Test: {test_n:,}")

def evaluate(preds):
    auc = BinaryClassificationEvaluator(labelCol="label", rawPredictionCol="rawPrediction",
                                        metricName="areaUnderROC").evaluate(preds)
    acc = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction",
                                            metricName="accuracy").evaluate(preds)
    f1 = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction",
                                           metricName="f1").evaluate(preds)
    prec = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction",
                                             metricName="weightedPrecision").evaluate(preds)
    rec = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction",
                                            metricName="weightedRecall").evaluate(preds)
    return {"auc": auc, "accuracy": acc, "f1": f1, "precision": prec, "recall": rec}

def cm(preds):
    tp = preds.filter((F.col("prediction") == 1) & (F.col("label") == 1)).count()
    tn = preds.filter((F.col("prediction") == 0) & (F.col("label") == 0)).count()
    fp = preds.filter((F.col("prediction") == 1) & (F.col("label") == 0)).count()
    fn = preds.filter((F.col("prediction") == 0) & (F.col("label") == 1)).count()
    return {"TP": tp, "TN": tn, "FP": fp, "FN": fn}

model_specs = [
    ("Logistic Regression", LogisticRegression(
        featuresCol="features", labelCol="label", weightCol="classWeight",
        maxIter=100, regParam=0.1, elasticNetParam=0.0
    ), {"maxIter": 100, "regParam": 0.1, "elasticNetParam": 0.0}),
    ("Decision Tree", DecisionTreeClassifier(
        featuresCol="features", labelCol="label", weightCol="classWeight",
        maxDepth=10, minInstancesPerNode=5
    ), {"maxDepth": 10, "minInstancesPerNode": 5}),
    ("Random Forest", RandomForestClassifier(
        featuresCol="features", labelCol="label", weightCol="classWeight",
        numTrees=100, maxDepth=8, seed=42
    ), {"numTrees": 100, "maxDepth": 8, "seed": 42}),
    ("GBT", GBTClassifier(
        featuresCol="features", labelCol="label", weightCol="classWeight",
        maxIter=50, maxDepth=6, stepSize=0.1
    ), {"maxIter": 50, "maxDepth": 6, "stepSize": 0.1}),
]

import numpy as np
from pyspark.sql.functions import udf
from pyspark.ml.linalg import Vectors, VectorUDT, SparseVector

def clip_neg(vec):
    if vec is None:
        return vec
    if isinstance(vec, SparseVector):
        new_vals = np.maximum(vec.values, 0.0)
        return Vectors.sparse(vec.size, vec.indices, new_vals.tolist())
    return Vectors.dense(np.maximum(vec.toArray(), 0.0).tolist())

clip_udf = udf(clip_neg, VectorUDT())
train_nb = train.withColumn("features_nn", clip_udf(F.col("features")))
test_nb = test.withColumn("features_nn", clip_udf(F.col("features")))

model_results = []

# Standard 4 models
for name, est, params in model_specs:
    print(f"[..] Training {name}...")
    with mlflow.start_run(run_name=name):
        mlflow.log_params(params)
        t0 = time.time()
        model = est.fit(train)
        dur = time.time() - t0
        preds = model.transform(test)
        m = evaluate(preds)
        c = cm(preds)
        mlflow.log_metrics(m)
        mlflow.log_metric("train_time_seconds", dur)
        try:
            mlflow.spark.log_model(model, "model")
        except Exception as e:
            print(f"  [warn] log_model failed: {e}")
        rec = {"model": name, **{k: round(v, 4) for k, v in m.items()},
               "train_time": round(dur, 2), "confusion": c}
        model_results.append(rec)
        print(f"[OK] {name}: AUC={m['auc']:.4f} F1={m['f1']:.4f} time={dur:.1f}s")

# Naive Bayes (negatif feature clip ile)
print("[..] Training Naive Bayes...")
with mlflow.start_run(run_name="Naive Bayes"):
    nb_params = {"smoothing": 1.0, "modelType": "multinomial"}
    mlflow.log_params(nb_params)
    nb = NaiveBayes(featuresCol="features_nn", labelCol="label", weightCol="classWeight",
                    smoothing=1.0, modelType="multinomial")
    t0 = time.time()
    model_nb = nb.fit(train_nb)
    dur = time.time() - t0
    preds_nb = model_nb.transform(test_nb)
    m = evaluate(preds_nb)
    c = cm(preds_nb)
    mlflow.log_metrics(m)
    mlflow.log_metric("train_time_seconds", dur)
    try:
        mlflow.spark.log_model(model_nb, "model")
    except Exception as e:
        print(f"  [warn] log_model failed: {e}")
    rec = {"model": "Naive Bayes", **{k: round(v, 4) for k, v in m.items()},
           "train_time": round(dur, 2), "confusion": c}
    model_results.append(rec)
    print(f"[OK] Naive Bayes: AUC={m['auc']:.4f} F1={m['f1']:.4f} time={dur:.1f}s")

# En iyi modeli bul
best = max(model_results, key=lambda r: r["auc"])
results["models"] = model_results
results["best_model"] = best
results["split"] = {"train": train_n, "test": test_n}
print(f"\n[OK] EN İYİ MODEL: {best['model']} (AUC={best['auc']})")

# ──────────────────────────────────────────────────────────────────────
# 8) SONUÇLARI KAYDET
# ──────────────────────────────────────────────────────────────────────
out_path = "/workspace/_results.json"
with open(out_path, "w", encoding="utf-8") as f:
    json.dump(results, f, indent=2, ensure_ascii=False)
print(f"\n[OK] Sonuçlar yazıldı: {out_path}")

spark.stop()
print("[DONE] Pipeline tamamlandı.")
