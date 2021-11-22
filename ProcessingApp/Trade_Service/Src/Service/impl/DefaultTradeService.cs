using ProcessingApp.Common.Src.Service.Utils;
using ProcessingApp.Trade_Service.Src.Domain.Utils;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reactive.Linq;
using System.Reactive.Subjects;
using ProcessingApp.Common.Src.Dto;
using ProcessingApp.Crypto_Service_Idl.Src.Service;
using ProcessingApp.Trade_Service_Idl.Src.Service;
using ProcessingApp.Trade_Service.Src.Domain;
using ProcessingApp.Trade_Service.Src.Repository;

namespace ProcessingApp.Trade_Service.Src.Service.impl
{
    public class DefaultTradeService : ITradeService
    {
        private readonly ILogger<DefaultTradeService> _logger;
        private readonly ICryptoService _cryptoService;
        private readonly IEnumerable<ITradeRepository> _tradeRepositories;

        public DefaultTradeService(
            ILogger<DefaultTradeService> logger,
            ICryptoService service,
            IEnumerable<ITradeRepository> tradeRepositories)
        {
            _logger = logger;
            _cryptoService = service ?? throw new ArgumentNullException(nameof(service));
            _tradeRepositories = tradeRepositories ?? throw new ArgumentNullException(nameof(tradeRepositories));
        }

        public IObservable<MessageDTO<MessageTrade>> TradesStream()
        {
            return _cryptoService.EventsStream()
                .Let(FilterAndMapTradingEvents)
                .Let(trades =>
                {
                    trades.Let(MapToDomainTrade)
                        .Let(f => ResilientlyStoreByBatchesToAllRepositories(f, _tradeRepositories.First(),
                            _tradeRepositories.Last()))
                        .Subscribe(new Subject<int>());

                    return trades;
                });
        }


        private IObservable<MessageDTO<MessageTrade>> FilterAndMapTradingEvents(
            IObservable<Dictionary<string, object>> input)
        {
            // TODO: Add implementation to produce trading events
            return input
                .Where(message => MessageMapper.IsTradeMessageType(message))
                .Select(message => MessageMapper.MapToTradeMessage(message));
        }

        private IObservable<Trade> MapToDomainTrade(IObservable<MessageDTO<MessageTrade>> input)
        {
            // TODO: Add implementation to mapping to com.example.part_10.domain.Trade
            return input.Select(message => DomainMapper.MapToDomain(message));
        }

        private IObservable<int> ResilientlyStoreByBatchesToAllRepositories(
                IObservable<Trade> input,
                ITradeRepository tradeRepository1,
                ITradeRepository tradeRepository2)
        {
            // behaviour subject unlike rerplay subject stores only one value
            // in always needs a default value
            var subject = new BehaviorSubject<object>(new object());
            subject.OnNext(new object()); // publishing value to subject

            IObservable<(long, object)> bufferBoundaries = Observable
                .Interval(TimeSpan.FromSeconds(1)) // generates values every specified period of time
                .Zip(subject, (i, o) => (i, o));
            // combines elements with the same index from 2 observables together
            // if one observable doesn't come for long the other one's elements wait

            // this way we can controll when to finish buffering
            // if we emit value in subject, new time range starts

            return input.Buffer(bufferBoundaries) // bufferizes observable values into list
                .SelectMany(trades => // works for all the gathered lists
                {
                    List<Trade> tradesList = trades.ToList();

                    return SaveSafely(tradeRepository1, tradesList) // saves to one db then saves to the other
                        .Merge(SaveSafely(tradeRepository2, tradesList))
                        .Do(onNext: i => { }, onCompleted: () => subject.OnNext(new object()));
                    // when both saves were successfull, emits new value to subject, starting new buffer

                });
        }

        private IObservable<int> SaveSafely(ITradeRepository tradeRepository, List<Trade> tradesList)
        {
            // if no result received during one second (timeout function) send error messages
            // Retry catches the error mesages, and tries again
            return tradeRepository
                .SaveAll(tradesList)
                .Timeout(TimeSpan.FromSeconds(1)).Retry(100);
        }
    }
}