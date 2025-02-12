﻿using System.Collections.Concurrent;
using System.Security.Cryptography.X509Certificates;
using Microsoft.VisualBasic;
using NumType = float;

var start = DateTime.Now;

const string folder = @"C:\Users\Kyle\Documents\source\OdinOneBillionRows\data";
const string filename = 
    //"measurements-1_000.txt";
    //"measurements-1_000_000.txt";
    "measurements.txt";
const string filepath = $"{folder}\\{filename}";
using var file = new StreamReader(filepath);

var stations = new ConcurrentDictionary<string, Station>();

while (true)
{
    var line = file.ReadLine();
    if (line is not null && line?.Length > 0)
    {
        ThreadPool.QueueUserWorkItem(ProcessLine, new WorkerContext(line, stations));
    }
    if (file.EndOfStream)
    {
        break;
    }
}

var end = DateTime.Now;
var runtime = end - start;

// bool first = false;
// foreach (var st in stations.Values)
// {
//     if (!first)
//         Console.Write(",");
//     else
//         first = false;

//     Console.Write(st.ToString());
// }
// Console.Write("\n");

Console.WriteLine($"Runtime: {runtime}");

static void ProcessLine(object? obj)
{
    var ctx = (WorkerContext)obj;

    var bits = ctx.Line.Split(';');
    if (bits.Length != 2) return;

    var name = bits[0];
    var vStr = bits[1];
    var vNum = NumType.Parse(vStr);

    var station = ctx.Stations.GetOrAdd(name, (key) => new Station(key));
    station.Update(vNum);
}

class WorkerContext(string line, ConcurrentDictionary<string, Station> stations)
{
    public string Line = line;
    public ConcurrentDictionary<string, Station> Stations = stations;
};

class Station
{
    public string Name;
    public uint Count;
    public NumType Sum;
    public NumType? Min;
    public NumType? Max;

    public NumType GetAvg() => Sum / Count;
    public override string ToString() => $"{Name}/{Count}/{Min}/{Max}/{GetAvg()}";

    public Station(string name)
    {
        Name = name;
        Count = 0;
        Sum = 0;
        Min = null;
        Max = null;
    }

    public void Update(NumType value)
    {
        Count += 1;
        Sum += value;
        if (Min is null || value < Min) Min = value;
        if (Max is null || value > Max) Max = value;
    }
}