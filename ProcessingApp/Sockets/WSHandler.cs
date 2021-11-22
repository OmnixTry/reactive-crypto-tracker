using System;
using System.Reactive.Linq;
using Microsoft.AspNetCore.Mvc;
using ProcessingApp.Price_Service_Idl.Src.Service;
using ProcessingApp.Trade_Service_Idl.Src.Service;

namespace ProcessingApp.Sockets
{
    public class WsHandler
    {
        private readonly IPriceService _priceService;
        private readonly ITradeService _tradeService;

        public WsHandler(
            IPriceService priceService,
            ITradeService tradeService)
        {
            _priceService = priceService;
            _tradeService = tradeService;
        }

        [HttpGet]
        public IObservable<dynamic> Handle(IObservable<string> inbound)
        {
            return inbound.Let(HandleRequestedAveragePriceIntervalValue)
                .Let(_priceService.PricesStream)
                .Merge<dynamic>(_tradeService.TradesStream());
        }

        private static IObservable<long> HandleRequestedAveragePriceIntervalValue(IObservable<string> requestedInterval)
        {
            // TODO: input may be incorrect, pass only correct interval
            // TODO: ignore invalid values (empty, non number, <= 0, > 60)
            return requestedInterval
                .Where(IsInputCorrect)
                .Select(i => long.Parse(i));
        }

        private static bool IsInputCorrect(string input)
		{
            const int MinSize = 0;
            const int MaxSize = 60;
            return !string.IsNullOrWhiteSpace(input) 
                && long.TryParse(input, out long interval) 
                && interval > MinSize && interval <= MaxSize;

        }
    }
}