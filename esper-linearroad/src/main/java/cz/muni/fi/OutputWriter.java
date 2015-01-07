package cz.muni.fi;

import cz.muni.fi.eventtypes.AccidentNotificationEvent;
import cz.muni.fi.eventtypes.AccountBalanceResponse;
import cz.muni.fi.eventtypes.DailyExpenditureResponse;
import cz.muni.fi.eventtypes.TollNotificationEvent;

/**
 * Exists only so we can pass a different implementation to DailyExpenditureProcess in the storm module
 */
public interface OutputWriter {

    public void outputAccidentNotification(AccidentNotificationEvent ane);
    public void outputTollNotification(TollNotificationEvent tne);
    public void outputDailyExpenditureResponse(DailyExpenditureResponse der);
    public void outputAccountBalanceResponse(AccountBalanceResponse abr);
    public void close();

}
