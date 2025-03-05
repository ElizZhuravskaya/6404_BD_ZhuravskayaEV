

package com.ververica.flinktraining.exercises.datastream_java.process;

import com.ververica.flinktraining.exercises.datastream_java.datatypes.TaxiFare;
import com.ververica.flinktraining.exercises.datastream_java.datatypes.TaxiRide;
import com.ververica.flinktraining.exercises.datastream_java.sources.TaxiFareSource;
import com.ververica.flinktraining.exercises.datastream_java.sources.TaxiRideSource;
import com.ververica.flinktraining.exercises.datastream_java.utils.ExerciseBase;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * Java reference implementation for the "Expiring State" exercise of the Flink training
 * (http://training.ververica.com).
 *
 * The goal for this exercise is to enrich TaxiRides with fare information.
 *
 * Parameters:
 * -rides path-to-input-file
 * -fares path-to-input-file
 *
 */
public class ExpiringStateExercise extends ExerciseBase {
	static final OutputTag<TaxiRide> unmatchedRides = new OutputTag<TaxiRide>("unmatchedRides") {};
	static final OutputTag<TaxiFare> unmatchedFares = new OutputTag<TaxiFare>("unmatchedFares") {};

	public static void main(String[] args) throws Exception {

		ParameterTool params = ParameterTool.fromArgs(args);
		final String ridesFile = params.get("rides", ExerciseBase.pathToRideData);
		final String faresFile = params.get("fares", ExerciseBase.pathToFareData);

		final int maxEventDelay = 60;           // events are out of order by max 60 seconds
		final int servingSpeedFactor = 600; 	// 10 minutes worth of events are served every second

		// set up streaming execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.setParallelism(ExerciseBase.parallelism);

		DataStream<TaxiRide> rides = env
				.addSource(rideSourceOrTest(new TaxiRideSource(ridesFile, maxEventDelay, servingSpeedFactor)))
				.filter((TaxiRide ride) -> (ride.isStart && (ride.rideId % 1000 != 0)))
				.keyBy(ride -> ride.rideId);

		DataStream<TaxiFare> fares = env
				.addSource(fareSourceOrTest(new TaxiFareSource(faresFile, maxEventDelay, servingSpeedFactor)))
				.keyBy(fare -> fare.rideId);

		SingleOutputStreamOperator processed = rides
				.connect(fares)
				.process(new EnrichmentFunction());

		printOrTest(processed.getSideOutput(unmatchedFares));

		env.execute("ExpiringStateSolution (java)");
	}

	public static class EnrichmentFunction extends KeyedCoProcessFunction<Long, TaxiRide, TaxiFare, Tuple2<TaxiRide, TaxiFare>> {
		// Хранимые состояния для поездок и тарифов
		private ValueState<TaxiRide> rideState;
		private ValueState<TaxiFare> fareState;

		@Override
		public void open(Configuration config) {
			// Создание состояний для хранения данных о поездке и тарифе
			rideState = getRuntimeContext().getState(new ValueStateDescriptor<>("rideRecord", TaxiRide.class));
			fareState = getRuntimeContext().getState(new ValueStateDescriptor<>("fareRecord", TaxiFare.class));
		}

		@Override
		public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<TaxiRide, TaxiFare>> out) throws Exception {
			// Проверяем, остались ли несопоставленные поездки или тарифы
			if (fareState.value() != null) {
				ctx.output(unmatchedFares, fareState.value());// Отправляем в побочный поток
				fareState.clear();
			}
			if (rideState.value() != null) {
				ctx.output(unmatchedRides, rideState.value());
				rideState.clear();
			}
		}

		@Override
		public void processElement1(TaxiRide ride, Context context, Collector<Tuple2<TaxiRide, TaxiFare>> out) throws Exception {
			TaxiFare fare = fareState.value();// Получаем сохраненный тариф

			if (fare != null) {
				// Если тариф найден, очищаем состояние и удаляем таймер
				fareState.clear();
				context.timerService().deleteEventTimeTimer(fare.getEventTime());
				out.collect(new Tuple2(ride, fare));// Отправляем объединенные данные
			} else {
				// Если тариф не найден, сохраняем поездку и ставим таймер
				rideState.update(ride);
				context.timerService().registerEventTimeTimer(ride.getEventTime());
			}
		}

		@Override
		public void processElement2(TaxiFare fare, Context context, Collector<Tuple2<TaxiRide, TaxiFare>> out) throws Exception {
			TaxiRide ride = rideState.value();// Получаем сохраненную поездку

			if (ride != null) {
				// Если поездка найдена, очищаем состояние и удаляем таймер
				rideState.clear();
				context.timerService().deleteEventTimeTimer(ride.getEventTime());
				out.collect(new Tuple2(ride, fare));
			} else {
				// Если поездка не найдена, сохраняем тариф и ставим таймер
				fareState.update(fare);
				context.timerService().registerEventTimeTimer(fare.getEventTime());
			}
		}
	}
}