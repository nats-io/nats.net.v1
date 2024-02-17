﻿// Copyright 2024 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using System;
using NATS.Client.Internals.SimpleJSON;

namespace NATSExamples
{
    public static class ChaosOutput
    {
        public static readonly object AppendLock = new object();

        static bool started = false;
        static bool ShowConsole = true;
        static bool ShowWork = false;
        static bool ShowDebug = false;
        static string controlConsoleAreaLabel = null;

        public static void Start(ChaosCommandLine cmd)
        {
            if (started)
            {
                return;
            }

            started = true;
            ShowWork = cmd.Work;
            ShowDebug = cmd.Debug;
            if (ShowConsole && (ShowWork || ShowDebug)) {
                controlConsoleAreaLabel = "CTRL";
            }

        }
        public static void WorkMessage(String label, String s) {
            if (ShowWork) {
                Append("WORK", label, s);
            }
        }

        public static void ControlMessage(String label, String s) {
            Append(controlConsoleAreaLabel, label, s);
        }

        public static void ErrorMessage(String label, String s)
        {
            Append("ERROR", label, s);
        }

        public static void FatalMessage(String label, String s)
        {
            Append("FATAL", label, s);
        }

        public static void Debug(String label, String s)
        {
            if (ShowDebug) {
                Append("DEBUG", label, s);
            }
        }

        const string NlIndent = "\n    ";
        public static void Append(string area, string label, string s) {
            lock (AppendLock)
            {
                Console.Write(Time());
                string llabel = label == null ? "" : " | " + label; 
                Console.Write(area == null ? llabel : " | " + area + llabel);

                if (s.Contains("\n"))
                {
                    if (!s.StartsWith("\n"))
                    {
                        Console.Write(" | ");
                    }
                    Console.Write(s.Replace("\n", NlIndent));
                }
                else
                {
                    Console.Write(" | ");
                    Console.Write(s);
                }

                Console.WriteLine();
            }
        }

        public static string Time()
        {
            return $"{DateTimeOffset.Now.ToUnixTimeMilliseconds()}".Substring(6);
        }
        
        public static String FN = "\n  ";
        public static String FBN = "{\n  ";
        public static String Formatted(JSONNode j) {
            return j.GetType().Name + j.ToString()
                .Replace("{\"", FBN + "\"").Replace(",", "," + FN);
        }

        public static String Formatted(Object o) {
            return Formatted(o.ToString());
        }

        public static String Formatted(String s) {
            return s.Replace("{", FBN).Replace(", ", "," + FN);
        }
    }
}
