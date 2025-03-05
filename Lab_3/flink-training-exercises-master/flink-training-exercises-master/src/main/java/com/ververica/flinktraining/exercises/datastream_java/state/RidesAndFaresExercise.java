/*
 * Copyright 2017 data Artisans GmbH, 2019 Ververica GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.flinktraining.exercises.datastream_java.state;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import com.ververica.flinktraining.exercises.datastream_java.datatypes.TaxiFare;
import com.ververica.flinktraining.exercises.datastream_java.datatypes.TaxiRide;
import com.ververica.flinktraining.exercises.datastream_java.sources.TaxiFareSource;
import com.ververica.flinktraining.exercises.datastream_java.sources.TaxiRideSource;
import com.ververica.flinktraining.exercises.datastream_java.utils.ExerciseBase;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;

/**
 * The "Stateful Enrichment" exercise of the Flink training
 * (http://training.ververica.com).
 *
 * The goal for this exercise is to enrich TaxiRides with fare information.
 *
 * Parameters:
 * -rides path-to-input-file
 * -fares path-to-input-file
 *
 */
// основной класс
public class RidesAndFaresExercise extends ExerciseBase {
	public static void main(String[] args) throws Exception {
		// Задаем пути для входных данных
		ParameterTool params = ParameterTool.fromArgs(args);
		final String ridesFile = params.get("rides", pathToRideData);
		final String faresFile = params.get("fares", pathToFareData);

		final int delay = 60;					// at most 60 seconds of delay
		final int servingSpeedFactor = 1800; 	// 30 minutes worth of events are served every second

		// Инициализация окружения
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(ExerciseBase.parallelism);

		DataStream<TaxiRide> rides = env
				.addSource(rideSourceOrTest(new TaxiRideSource(ridesFile, delay, servingSpeedFactor)))
				.filter((TaxiRide ride) -> ride.isStart)
				.keyBy("rideId");

		DataStream<TaxiFare> fares = env
				.addSource(fareSourceOrTest(new TaxiFareSource(faresFile, delay, servingSpeedFactor)))
				.keyBy("rideId");

		DataStream<Tuple2<TaxiRide, TaxiFare>> enrichedRides = rides
				.connect(fares)
				.flatMap(new EnrichmentFunction());

		printOrTest(enrichedRides);

		env.execute("Join Rides with Fares (java RichCoFlatMap)");
	}
	public static class EnrichmentFunction extends RichCoFlatMapFunction<TaxiRide, TaxiFare, Tuple2<TaxiRide, TaxiFare>> {

		// MapState для хранения информации о тарифах
		private transient MapState<Long, TaxiFare> fareState;

		@Override
		public void open(Configuration config) throws Exception {
			// Инициализируем состояние для сохранения тарифов
			MapStateDescriptor<Long, TaxiFare> descriptor =
					new MapStateDescriptor<>(
							"fareState", // имя состояния
							BasicTypeInfo.LONG_TYPE_INFO, // тип ключа (rideId)
							TypeInformation.of(TaxiFare.class) // тип значения (TaxiFare - информация о тарифе)
					);
			// Получаем доступ к состоянию через RuntimeContext
			fareState = getRuntimeContext().getMapState(descriptor);
		}

		@Override
		// Ищет тариф для каждого rideId и если есть, то добавляет
		public void flatMap1(TaxiRide ride, Collector<Tuple2<TaxiRide, TaxiFare>> out) throws Exception {
			TaxiFare fare = fareState.get(ride.rideId);
			if (fare != null) {
				// Выводим пару данных (поездка + тариф) в выходной поток
				out.collect(new Tuple2<>(ride, fare));
			}
		}

		@Override
		// Сохраняет инфу о тарифе и состояние
		public void flatMap2(TaxiFare fare, Collector<Tuple2<TaxiRide, TaxiFare>> out) throws Exception {
			fareState.put(fare.rideId, fare);
		}
	}

}