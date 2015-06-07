package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.Context;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;

/**
 * Created by prasan on 4/21/15.
 */
public class DBHelperClass extends SQLiteOpenHelper {

    public static final String TABLE_NAME = "MESSAGE_ORDER";
    public static final String COLUMN_NAME1 = "key";
    public static final String COLUMN_NAME2 = "value";
    public static final String COLUMN_NAME3 = "node";
    public static final String DB_NAME = "MESSAGE_ORDER_DATABASE";
    public static final int DB_VERSION = 2;

    private static final String CREATE_TABLE = "create table " + TABLE_NAME + " (" + COLUMN_NAME1
            + " TEXT PRIMARY KEY NOT NULL, " + COLUMN_NAME2 + " TEXT NOT NULL, " + COLUMN_NAME3 + " TEXT " + ");";

    public DBHelperClass(Context context) {
        super(context, DB_NAME, null, DB_VERSION);
    }

    @Override
    public void onCreate(SQLiteDatabase db) {
        db.execSQL(CREATE_TABLE);
        System.out.println("Database created");
    }

    @Override
    public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {
        db.execSQL("DROP TABLE IF EXISTS " + TABLE_NAME);
        onCreate(db);
    }

}
