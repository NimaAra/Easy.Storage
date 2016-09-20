namespace Easy.Storage.Tests.Unit.Sqlite.FTS
{
    using Easy.Storage.Sqlite.FTS;
    using NUnit.Framework;
    using Shouldly;

    [TestFixture]
    internal sealed class TermTests : FtsContext
    {
        [Test]
        public void When_creating_default_term()
        {
            var term = Term<Log>.All;
            term.ToString().ShouldBe("SELECT docId FROM Log_fts WHERE 1=1");
        }
        
        [Test]
        public void When_creating_term_with_no_value_for_column()
        {
            var term = Term<Log>.All;

            term.And(Match.Any, l => l.Message);
            term.ToString().ShouldBe("SELECT docId FROM Log_fts WHERE [Message] MATCH ''");
        }

        [Test]
        public void When_creating_term_with_single_column_matching_none_of_any()
        {
            var term = Term<Log>.All;

            term.AndNot(Match.Any, l => l.Message, "One", "Two");
            term.ToString().ShouldBe("SELECT docId FROM Log_fts WHERE docId NOT IN"
                + "\r\n("
                + "\r\n    SELECT docId FROM Log_fts WHERE [Message] MATCH '\"One\" OR \"Two\"'"
                + "\r\n)");
        }

        [Test]
        public void When_creating_term_with_single_column_matching_none_of_all()
        {
            var term = Term<Log>.All;
            
            term.AndNot(Match.All, l => l.Message, "One", "Two");
            term.ToString().ShouldBe("SELECT docId FROM Log_fts WHERE docId NOT IN"
                + "\r\n("
                + "\r\n    SELECT docId FROM Log_fts WHERE [Message] MATCH '\"One\" \"Two\"'"
                + "\r\n)");
        }

        [Test]
        public void When_creating_term_with_single_column_matching_any()
        {
            var term = Term<Log>.All;

            term.And(Match.Any, l => l.Message, "One", "Two");
            term.ToString().ShouldBe("SELECT docId FROM Log_fts WHERE [Message] MATCH '\"One\" OR \"Two\"'");
        }

        [Test]
        public void When_creating_term_with_single_column_matching_all()
        {
            var term = Term<Log>.All;

            term.And(Match.All, l => l.Message, "One", "Two");
            term.ToString().ShouldBe("SELECT docId FROM Log_fts WHERE [Message] MATCH '\"One\" \"Two\"'");
        }

        [Test]
        public void When_creating_term_with_and_clause_matching_any()
        {
            var term = Term<Log>.All;

            term.And(Match.Any, l => l.Message, "One", "Two")
                .And(Match.Any, l => l.Level, Level.Debug, Level.Error);

            term.ToString().ShouldBe("SELECT docId FROM Log_fts WHERE [Message] MATCH '\"One\" OR \"Two\"'"
                + "\r\nINTERSECT\r\n"
                + "SELECT docId FROM Log_fts WHERE [Level] MATCH '\"0\" OR \"3\"'");
        }

        [Test]
        public void When_creating_term_with_and_clause_matching_all()
        {
            var term = Term<Log>.All;

            term.And(Match.All, l => l.Message, "One", "Two")
                .And(Match.All, l => l.Level, Level.Debug, Level.Error);

            term.ToString().ShouldBe("SELECT docId FROM Log_fts WHERE [Message] MATCH '\"One\" \"Two\"'"
                + "\r\nINTERSECT\r\n"
                + "SELECT docId FROM Log_fts WHERE [Level] MATCH '\"0\" \"3\"'");
        }

        [Test]
        public void When_creating_term_with_or_clause_matching_any()
        {
            var term = Term<Log>.All;

            term.And(Match.Any, l => l.Message, "One", "Two")
                .Or(Match.Any, l => l.Level, Level.Debug, Level.Error);

            term.ToString().ShouldBe("SELECT docId FROM Log_fts WHERE [Message] MATCH '\"One\" OR \"Two\"'"
                + "\r\nUNION\r\n"
                + "SELECT docId FROM Log_fts WHERE [Level] MATCH '\"0\" OR \"3\"'");
        }

        [Test]
        public void When_creating_term_with_or_clause_matching_all()
        {
            var term = Term<Log>.All;

            term.And(Match.All, l => l.Message, "One", "Two")
                .Or(Match.All, l => l.Level, Level.Debug, Level.Error);

            term.ToString().ShouldBe("SELECT docId FROM Log_fts WHERE [Message] MATCH '\"One\" \"Two\"'"
                + "\r\nUNION\r\n"
                + "SELECT docId FROM Log_fts WHERE [Level] MATCH '\"0\" \"3\"'");
        }

        [Test]
        public void When_creating_term_starting_with_match_then_or_match_then_or_none()
        {
            var term = Term<Log>.All;

            term.And(Match.All, l => l.Message, "One", "Two")
                .Or(Match.All, l => l.Level, Level.Debug, Level.Error)
                .OrNot(Match.All, l => l.Message, "Foo", "Bar");

            term.ToString().ShouldBe("SELECT docId FROM Log_fts WHERE [Message] MATCH '\"One\" \"Two\"'"
                + "\r\nUNION\r\n"
                + "SELECT docId FROM Log_fts WHERE [Level] MATCH '\"0\" \"3\"'"
                + "\r\nUNION\r\n"
                + "SELECT docId FROM Log_fts WHERE docId NOT IN"
                + "\r\n("
                + "\r\n    SELECT docId FROM Log_fts WHERE [Message] MATCH '\"Foo\" \"Bar\"'"
                + "\r\n)");
        }

        [Test]
        public void When_creating_term_starting_with_no_match_then_and_match_then_or_match()
        {
            var term = Term<Log>.All;

            term.AndNot(Match.Any, l => l.Message, "Foo", "Bar")
                .And(Match.Any, l => l.Level, Level.Debug, Level.Error)
                .Or(Match.Any, l => l.Message, "One", "Two");

            term.ToString().ShouldBe("SELECT docId FROM Log_fts WHERE docId NOT IN"
                + "\r\n("
                + "\r\n    SELECT docId FROM Log_fts WHERE [Message] MATCH '\"Foo\" OR \"Bar\"'"
                + "\r\n)"
                + "\r\nINTERSECT\r\n"
                + "SELECT docId FROM Log_fts WHERE [Level] MATCH '\"0\" OR \"3\"'"
                + "\r\nUNION\r\n"
                + "SELECT docId FROM Log_fts WHERE [Message] MATCH '\"One\" OR \"Two\"'");
        }

        [Test]
        public void When_creating_term_with_multiple_clause_with_and_not()
        {
            var term = Term<Log>.All;

            term.And(Match.Any, l => l.Message, "Foo", "Bar")
                .And(Match.Any, l => l.Level, Level.Debug, Level.Error)
                .Or(Match.Any, l => l.Message, "One", "Two")
                .AndNot(Match.Any, l => l.Message, "Buzz", "Bizz");

            term.ToString().ShouldBe("SELECT docId FROM Log_fts WHERE [Message] MATCH '\"Foo\" OR \"Bar\"'"
                + "\r\nINTERSECT\r\n"
                + "SELECT docId FROM Log_fts WHERE [Level] MATCH '\"0\" OR \"3\"'"
                + "\r\nUNION\r\n"
                + "SELECT docId FROM Log_fts WHERE [Message] MATCH '\"One\" OR \"Two\"'"
                + "\r\nINTERSECT\r\n"
                + "SELECT docId FROM Log_fts WHERE docId NOT IN"
                + "\r\n("
                + "\r\n    SELECT docId FROM Log_fts WHERE [Message] MATCH '\"Buzz\" OR \"Bizz\"'"
                + "\r\n)");
        }

        [Test]
        public void When_clearing_a_term()
        {
            var term = Term<Log>.All;

            term.AndNot(Match.Any, l => l.Message, "Foo", "Bar")
                .And(Match.Any, l => l.Level, Level.Debug, Level.Error)
                .Or(Match.Any, l => l.Message, "One", "Two");

            term.ToString().ShouldBe("SELECT docId FROM Log_fts WHERE docId NOT IN"
                + "\r\n("
                + "\r\n    SELECT docId FROM Log_fts WHERE [Message] MATCH '\"Foo\" OR \"Bar\"'"
                + "\r\n)"
                + "\r\nINTERSECT\r\n"
                + "SELECT docId FROM Log_fts WHERE [Level] MATCH '\"0\" OR \"3\"'"
                + "\r\nUNION\r\n"
                + "SELECT docId FROM Log_fts WHERE [Message] MATCH '\"One\" OR \"Two\"'");

            term.Clear();

            term.ToString().ShouldBe("SELECT docId FROM Log_fts WHERE 1=1");
        }
    }
}