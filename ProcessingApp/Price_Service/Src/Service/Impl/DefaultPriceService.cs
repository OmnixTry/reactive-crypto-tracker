using ProcessingApp.Common.Src.Service.Utils;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Reactive.Linq;
using ProcessingApp.Common.Src.Dto;
using ProcessingApp.Crypto_Service_Idl.Src.Service;
using ProcessingApp.Price_Service_Idl.Src.Service;

namespace ProcessingApp.Price_Service.Src.Service.Impl
{
    public class DefaultPriceService : IPriceService
    {
        private static readonly long DEFAULT_AVG_PRICE_INTERVAL = 30L;

        private readonly ILogger<DefaultPriceService> _logger;
        private readonly ICryptoService _cryptoService;

        private IObservable<MessageDTO<float>> SharedStream => _cryptoService.EventsStream()
            .Let(SelectOnlyPriceUpdateEvents)
            .Let(CurrentPrice);

        public DefaultPriceService(ILogger<DefaultPriceService> logger, ICryptoService cryptoService)
        {
            _logger = logger;
            _cryptoService = cryptoService ?? throw new ArgumentNullException(nameof(cryptoService));
        }

        public IObservable<MessageDTO<float>> PricesStream(IObservable<long> intervalPreferencesStream)
        {
            return SharedStream.Merge(AveragePrice(intervalPreferencesStream, SharedStream));
        }

        // FIXME:
        // 1) JUST FOR WARM UP: .map() incoming Dictionary<string, object> to MessageDTO. For that purpose use MessageDTO.price()
        //    NOTE: Incoming Dictionary<string, object> contains keys PRICE_KEY and CURRENCY_KEY
        //    NOTE: Use MessageMapper utility class for message validation and transformation
        // Visible for testing
        private static IObservable<Dictionary<string, object>> SelectOnlyPriceUpdateEvents(
            IObservable<Dictionary<string, object>> input)
        {
            // TODO: filter only Price messages
            // TODO: verify that price message are valid
            // HINT: Use MessageMapper methods to perform filtering and validation

            // used Linq syntax for filtering operation
            return input
                .Where(m => { return MessageMapper.IsPriceMessageType(m) 
                    && MessageMapper.IsValidPriceMessage(m); });
        }

        // Visible for testing
        private static IObservable<MessageDTO<float>> CurrentPrice(IObservable<Dictionary<string, object>> input)
        {
            // TODO map to Statistic message using MessageMapper.mapToPriceMessage

            return input.Select(m => MessageMapper.MapToPriceMessage(m));
        }

        // 1.1)   TODO Collect crypto currency price during the interval of seconds
        //        HINT consider corner case when a client did not send any info about interval (add initial interval (mergeWith(...)))
        //        HINT use window + switchMap
        // 1.2)   TODO group collected MessageDTO results by currency
        //        HINT for reduce consider to reuse Sum.empty and Sum#add
        // 1.3.2) TODO calculate average for reduced Sum object using Sun#avg
        // 1.3.3) TODO map to Statistic message using MessageDTO#avg()

        //             |   |
        //             |   |
        //         ____|   |____
        //        |             |
        //        |             |
        //        |             |
        //        |____     ____|
        //             |   |
        //             |   |
        //             |   |


        // Visible for testing
        // TODO: Remove as should be implemented by trainees
        private static IObservable<MessageDTO<float>> AveragePrice(IObservable<long> requestedInterval,
            IObservable<MessageDTO<float>> priceData)
        {
            // Return just creates an observable emitting 1 element
            // using it in case client didn't save any information (so called Corner Case)
            // after the client sends new window it will be applied
            return Observable.Concat(Observable.Return(DEFAULT_AVG_PRICE_INTERVAL), requestedInterval)
                .Select(timeInterval => // 5
                    priceData
                        .Window(TimeSpan.FromSeconds(timeInterval)) // splits observables into groups with time interval
                                                                // (makes the Observable<Observable<>>)
                        .SelectMany(priceMessage => // turns observable sequences into different sequences and merges them
                            priceMessage
                                .GroupBy(m => m.Currency) // groups observables and finds averagin for every currency
                                .SelectMany(grouped => grouped
                                    .Aggregate(Sum.Empty(), (sum, mes) => sum.Add(mes.Data), sum => sum.Avg())
                                    .Select(avg => MessageDTO<float>.Avg(avg, grouped.Key, "Local"))
                            ))
                )
                .Switch();
        }
    }
}