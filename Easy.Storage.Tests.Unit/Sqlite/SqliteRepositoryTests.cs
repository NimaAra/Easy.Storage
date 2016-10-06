namespace Easy.Storage.Tests.Unit.Sqlite
{
    using System;
    using System.Linq;
    using System.Text;
    using System.Threading.Tasks;
    using Easy.Storage.Common;
    using Easy.Storage.Sqlite;
    using Easy.Storage.Tests.Unit.Models;
    using NUnit.Framework;
    using Shouldly;
    using Easy.Storage.Common.Extensions;

    [TestFixture]
    internal sealed class SqliteRepositoryTests : Context
    {
        [Test]
        public async Task When_checking_if_table_exists_non_aliased_models()
        {
            using (IDatabase db = new SqliteDatabase("Data Source=:memory:"))
            {
                (await db.ExistsAsync<Person>()).ShouldBeFalse();
                await db.Connection.ExecuteAsync(TableQuery);
                (await db.ExistsAsync<Person>()).ShouldBeTrue();
            }
        }

        [Test]
        public async Task When_checking_if_table_exists_aliased_models()
        {
            using (IDatabase db = new SqliteDatabase("Data Source=:memory:"))
            {
                (await db.ExistsAsync<MyPerson>()).ShouldBeFalse();
                await db.Connection.ExecuteAsync(TableQuery);
                (await db.ExistsAsync<MyPerson>()).ShouldBeTrue();
            }
        }

        [Test]
        public async Task When_getting_non_aliased_models_lazily()
        {
            using (IDatabase db = new SqliteDatabase("Data Source=:memory:"))
            {
                var repo = db.GetRepository<Person>();

                await db.Connection.ExecuteAsync(TableQuery);

                (await repo.GetLazyAsync()).ShouldBeEmpty();

                var people = new[]
                {
                    new Person { Name = "P1", Age = 10 },
                    new Person { Name = "P2", Age = 20 },
                    new Person { Id = 123, Name = "P3", Age = 30 },
                    new Person { Name = "P4", Age = 40 }
                };

                (await repo.InsertAsync(people)).ShouldBe(4);

                var insertedPeopleBuffered = (await repo.GetLazyAsync()).ToArray();
                var insertedPeopleUnBuffered = (await repo.GetLazyAsync()).ToArray();

                insertedPeopleBuffered.Length.ShouldBe(people.Length);
                insertedPeopleUnBuffered.Length.ShouldBe(people.Length);

                insertedPeopleBuffered[0].Id.ShouldBe(1);
                insertedPeopleBuffered[0].Name.ShouldBe("P1");
                insertedPeopleBuffered[0].Age.ShouldBe(10);

                insertedPeopleBuffered[1].Id.ShouldBe(2);
                insertedPeopleBuffered[1].Name.ShouldBe("P2");
                insertedPeopleBuffered[1].Age.ShouldBe(20);

                insertedPeopleBuffered[2].Id.ShouldBe(3);
                insertedPeopleBuffered[2].Name.ShouldBe("P3");
                insertedPeopleBuffered[2].Age.ShouldBe(30);

                insertedPeopleBuffered[3].Id.ShouldBe(4);
                insertedPeopleBuffered[3].Name.ShouldBe("P4");
                insertedPeopleBuffered[3].Age.ShouldBe(40);

                insertedPeopleUnBuffered[0].Id.ShouldBe(1);
                insertedPeopleUnBuffered[0].Name.ShouldBe("P1");
                insertedPeopleUnBuffered[0].Age.ShouldBe(10);

                insertedPeopleUnBuffered[1].Id.ShouldBe(2);
                insertedPeopleUnBuffered[1].Name.ShouldBe("P2");
                insertedPeopleUnBuffered[1].Age.ShouldBe(20);

                insertedPeopleUnBuffered[2].Id.ShouldBe(3);
                insertedPeopleUnBuffered[2].Name.ShouldBe("P3");
                insertedPeopleUnBuffered[2].Age.ShouldBe(30);

                insertedPeopleUnBuffered[3].Id.ShouldBe(4);
                insertedPeopleUnBuffered[3].Name.ShouldBe("P4");
                insertedPeopleUnBuffered[3].Age.ShouldBe(40);
            }
        }

        [Test]
        public async Task When_getting_aliased_models_lazily()
        {
            using (IDatabase db = new SqliteDatabase("Data Source=:memory:"))
            {
                var repo = db.GetRepository<MyPerson>();

                await db.Connection.ExecuteAsync(TableQuery);

                (await repo.GetLazyAsync()).ShouldBeEmpty();

                var people = new[]
                {
                    new MyPerson { SomeName = "P1", Age = 10 },
                    new MyPerson { SomeName = "P2", Age = 20 },
                    new MyPerson { SomeId = 123, SomeName = "P3", Age = 30 },
                    new MyPerson { SomeName = "P4", Age = 40 }
                };

                (await repo.InsertAsync(people)).ShouldBe(4);

                var insertedPeopleBuffered = (await repo.GetLazyAsync()).ToArray();
                var insertedPeopleUnBuffered = (await repo.GetLazyAsync()).ToArray();

                insertedPeopleBuffered.Length.ShouldBe(people.Length);
                insertedPeopleUnBuffered.Length.ShouldBe(people.Length);

                insertedPeopleBuffered[0].SomeId.ShouldBe(1);
                insertedPeopleBuffered[0].SomeName.ShouldBe("P1");
                insertedPeopleBuffered[0].Age.ShouldBe(10);

                insertedPeopleBuffered[1].SomeId.ShouldBe(2);
                insertedPeopleBuffered[1].SomeName.ShouldBe("P2");
                insertedPeopleBuffered[1].Age.ShouldBe(20);

                insertedPeopleBuffered[2].SomeId.ShouldBe(3);
                insertedPeopleBuffered[2].SomeName.ShouldBe("P3");
                insertedPeopleBuffered[2].Age.ShouldBe(30);

                insertedPeopleBuffered[3].SomeId.ShouldBe(4);
                insertedPeopleBuffered[3].SomeName.ShouldBe("P4");
                insertedPeopleBuffered[3].Age.ShouldBe(40);

                insertedPeopleUnBuffered[0].SomeId.ShouldBe(1);
                insertedPeopleUnBuffered[0].SomeName.ShouldBe("P1");
                insertedPeopleUnBuffered[0].Age.ShouldBe(10);

                insertedPeopleUnBuffered[1].SomeId.ShouldBe(2);
                insertedPeopleUnBuffered[1].SomeName.ShouldBe("P2");
                insertedPeopleUnBuffered[1].Age.ShouldBe(20);

                insertedPeopleUnBuffered[2].SomeId.ShouldBe(3);
                insertedPeopleUnBuffered[2].SomeName.ShouldBe("P3");
                insertedPeopleUnBuffered[2].Age.ShouldBe(30);

                insertedPeopleUnBuffered[3].SomeId.ShouldBe(4);
                insertedPeopleUnBuffered[3].SomeName.ShouldBe("P4");
                insertedPeopleUnBuffered[3].Age.ShouldBe(40);
            }
        }

        [Test]
        public async Task When_getting_non_aliased_models()
        {
            using (IDatabase db = new SqliteDatabase("Data Source=:memory:"))
            {
                var repo = db.GetRepository<Person>();

                await db.Connection.ExecuteAsync(TableQuery);

                (await repo.GetAsync()).ShouldBeEmpty();

                var people = new[]
                {
                    new Person { Name = "P1", Age = 10 },
                    new Person { Name = "P2", Age = 20 },
                    new Person { Id = 123, Name = "P3", Age = 30 },
                    new Person { Name = "P4", Age = 40 }
                };

                (await repo.InsertAsync(people)).ShouldBe(4);

                var insertedPeopleBuffered = (await repo.GetAsync()).ToArray();
                var insertedPeopleUnBuffered = (await repo.GetAsync()).ToArray();

                insertedPeopleBuffered.Length.ShouldBe(people.Length);
                insertedPeopleUnBuffered.Length.ShouldBe(people.Length);

                insertedPeopleBuffered[0].Id.ShouldBe(1);
                insertedPeopleBuffered[0].Name.ShouldBe("P1");
                insertedPeopleBuffered[0].Age.ShouldBe(10);

                insertedPeopleBuffered[1].Id.ShouldBe(2);
                insertedPeopleBuffered[1].Name.ShouldBe("P2");
                insertedPeopleBuffered[1].Age.ShouldBe(20);

                insertedPeopleBuffered[2].Id.ShouldBe(3);
                insertedPeopleBuffered[2].Name.ShouldBe("P3");
                insertedPeopleBuffered[2].Age.ShouldBe(30);

                insertedPeopleBuffered[3].Id.ShouldBe(4);
                insertedPeopleBuffered[3].Name.ShouldBe("P4");
                insertedPeopleBuffered[3].Age.ShouldBe(40);

                insertedPeopleUnBuffered[0].Id.ShouldBe(1);
                insertedPeopleUnBuffered[0].Name.ShouldBe("P1");
                insertedPeopleUnBuffered[0].Age.ShouldBe(10);

                insertedPeopleUnBuffered[1].Id.ShouldBe(2);
                insertedPeopleUnBuffered[1].Name.ShouldBe("P2");
                insertedPeopleUnBuffered[1].Age.ShouldBe(20);

                insertedPeopleUnBuffered[2].Id.ShouldBe(3);
                insertedPeopleUnBuffered[2].Name.ShouldBe("P3");
                insertedPeopleUnBuffered[2].Age.ShouldBe(30);

                insertedPeopleUnBuffered[3].Id.ShouldBe(4);
                insertedPeopleUnBuffered[3].Name.ShouldBe("P4");
                insertedPeopleUnBuffered[3].Age.ShouldBe(40);
            }
        }

        [Test]
        public async Task When_getting_aliased_models()
        {
            using (IDatabase db = new SqliteDatabase("Data Source=:memory:"))
            {
                var repo = db.GetRepository<MyPerson>();

                await db.Connection.ExecuteAsync(TableQuery);

                (await repo.GetAsync()).ShouldBeEmpty();

                var people = new[]
                {
                    new MyPerson { SomeName = "P1", Age = 10 },
                    new MyPerson { SomeName = "P2", Age = 20 },
                    new MyPerson { SomeId = 123, SomeName = "P3", Age = 30 },
                    new MyPerson { SomeName = "P4", Age = 40 }
                };

                (await repo.InsertAsync(people)).ShouldBe(4);

                var insertedPeopleBuffered = (await repo.GetAsync()).ToArray();
                var insertedPeopleUnBuffered = (await repo.GetAsync()).ToArray();

                insertedPeopleBuffered.Length.ShouldBe(people.Length);
                insertedPeopleUnBuffered.Length.ShouldBe(people.Length);

                insertedPeopleBuffered[0].SomeId.ShouldBe(1);
                insertedPeopleBuffered[0].SomeName.ShouldBe("P1");
                insertedPeopleBuffered[0].Age.ShouldBe(10);

                insertedPeopleBuffered[1].SomeId.ShouldBe(2);
                insertedPeopleBuffered[1].SomeName.ShouldBe("P2");
                insertedPeopleBuffered[1].Age.ShouldBe(20);

                insertedPeopleBuffered[2].SomeId.ShouldBe(3);
                insertedPeopleBuffered[2].SomeName.ShouldBe("P3");
                insertedPeopleBuffered[2].Age.ShouldBe(30);

                insertedPeopleBuffered[3].SomeId.ShouldBe(4);
                insertedPeopleBuffered[3].SomeName.ShouldBe("P4");
                insertedPeopleBuffered[3].Age.ShouldBe(40);

                insertedPeopleUnBuffered[0].SomeId.ShouldBe(1);
                insertedPeopleUnBuffered[0].SomeName.ShouldBe("P1");
                insertedPeopleUnBuffered[0].Age.ShouldBe(10);

                insertedPeopleUnBuffered[1].SomeId.ShouldBe(2);
                insertedPeopleUnBuffered[1].SomeName.ShouldBe("P2");
                insertedPeopleUnBuffered[1].Age.ShouldBe(20);

                insertedPeopleUnBuffered[2].SomeId.ShouldBe(3);
                insertedPeopleUnBuffered[2].SomeName.ShouldBe("P3");
                insertedPeopleUnBuffered[2].Age.ShouldBe(30);

                insertedPeopleUnBuffered[3].SomeId.ShouldBe(4);
                insertedPeopleUnBuffered[3].SomeName.ShouldBe("P4");
                insertedPeopleUnBuffered[3].Age.ShouldBe(40);
            }
        }

        [Test]
        public async Task When_getting_non_aliased_models_by_selector()
        {
            using (IDatabase db = new SqliteDatabase("Data Source=:memory:"))
            {
                var repo = db.GetRepository<Person>();

                await db.Connection.ExecuteAsync(TableQuery);

                (await repo.GetAsync()).ShouldBeEmpty();

                var people = new[]
                {
                    new Person { Name = "P1", Age = 10 },
                    new Person { Name = "P2", Age = 20 },
                    new Person { Id = 123, Name = "P3", Age = 30 },
                    new Person { Name = "P4", Age = 10 },
                    new Person { Name = "P5", Age = 40 },
                    new Person { Name = "P5", Age = 10 }
                };

                (await repo.InsertAsync(people)).ShouldBe(6);

                var onePerson = (await repo.GetAsync(p => p.Id, 1)).ToArray();

                onePerson.Length.ShouldBe(1);
                onePerson[0].Id.ShouldBe(1);
                onePerson[0].Name.ShouldBe("P1");
                onePerson[0].Age.ShouldBe(10);

                var peopleWithSameName = (await repo.GetAsync(p => p.Name, "P5")).ToArray();

                peopleWithSameName.Length.ShouldBe(2);
                peopleWithSameName[0].Id.ShouldBe(5);
                peopleWithSameName[0].Name.ShouldBe("P5");
                peopleWithSameName[0].Age.ShouldBe(40);

                peopleWithSameName[1].Id.ShouldBe(6);
                peopleWithSameName[1].Name.ShouldBe("P5");
                peopleWithSameName[1].Age.ShouldBe(10);

                var peopleWithSameAge = (await repo.GetAsync(p => p.Age, 10)).ToArray();

                peopleWithSameAge.Length.ShouldBe(3);
                peopleWithSameAge[0].Id.ShouldBe(1);
                peopleWithSameAge[0].Name.ShouldBe("P1");
                peopleWithSameAge[0].Age.ShouldBe(10);

                peopleWithSameAge[1].Id.ShouldBe(4);
                peopleWithSameAge[1].Name.ShouldBe("P4");
                peopleWithSameAge[1].Age.ShouldBe(10);

                peopleWithSameAge[2].Id.ShouldBe(6);
                peopleWithSameAge[2].Name.ShouldBe("P5");
                peopleWithSameAge[2].Age.ShouldBe(10);

                var peopleWithDifferentNames = (await repo.GetAsync(p => p.Name, null, "P1", "P2")).ToArray();

                peopleWithDifferentNames.Length.ShouldBe(2);

                peopleWithDifferentNames[0].Id.ShouldBe(1);
                peopleWithDifferentNames[0].Name.ShouldBe("P1");
                peopleWithDifferentNames[0].Age.ShouldBe(10);

                peopleWithDifferentNames[1].Id.ShouldBe(2);
                peopleWithDifferentNames[1].Name.ShouldBe("P2");
                peopleWithDifferentNames[1].Age.ShouldBe(20);

                var nonExistingPeople = (await repo.GetAsync(p => p.Age, 60)).ToArray();

                nonExistingPeople.ShouldBeEmpty();
            }
        }

        [Test]
        public async Task When_getting_aliased_models_by_selector()
        {
            using (IDatabase db = new SqliteDatabase("Data Source=:memory:"))
            {
                var repo = db.GetRepository<MyPerson>();

                await db.Connection.ExecuteAsync(TableQuery);

                (await repo.GetAsync()).ShouldBeEmpty();

                var people = new[]
                {
                    new MyPerson { SomeName = "P1", Age = 10 },
                    new MyPerson { SomeName = "P2", Age = 20 },
                    new MyPerson { SomeId = 123, SomeName = "P3", Age = 30 },
                    new MyPerson { SomeName = "P4", Age = 10 },
                    new MyPerson { SomeName = "P5", Age = 40 },
                    new MyPerson { SomeName = "P5", Age = 10 }
                };

                (await repo.InsertAsync(people)).ShouldBe(6);

                var onePerson = (await repo.GetAsync(p => p.SomeId, 1)).ToArray();

                onePerson.Length.ShouldBe(1);
                onePerson[0].SomeId.ShouldBe(1);
                onePerson[0].SomeName.ShouldBe("P1");
                onePerson[0].Age.ShouldBe(10);

                var peopleWithSameName = (await repo.GetAsync(p => p.SomeName, "P5")).ToArray();

                peopleWithSameName.Length.ShouldBe(2);
                peopleWithSameName[0].SomeId.ShouldBe(5);
                peopleWithSameName[0].SomeName.ShouldBe("P5");
                peopleWithSameName[0].Age.ShouldBe(40);

                peopleWithSameName[1].SomeId.ShouldBe(6);
                peopleWithSameName[1].SomeName.ShouldBe("P5");
                peopleWithSameName[1].Age.ShouldBe(10);

                var peopleWithSameAge = (await repo.GetAsync(p => p.Age, 10)).ToArray();

                peopleWithSameAge.Length.ShouldBe(3);
                peopleWithSameAge[0].SomeId.ShouldBe(1);
                peopleWithSameAge[0].SomeName.ShouldBe("P1");
                peopleWithSameAge[0].Age.ShouldBe(10);

                peopleWithSameAge[1].SomeId.ShouldBe(4);
                peopleWithSameAge[1].SomeName.ShouldBe("P4");
                peopleWithSameAge[1].Age.ShouldBe(10);

                peopleWithSameAge[2].SomeId.ShouldBe(6);
                peopleWithSameAge[2].SomeName.ShouldBe("P5");
                peopleWithSameAge[2].Age.ShouldBe(10);

                var peopleWithDifferentNames = (await repo.GetAsync(p => p.SomeName, null, "P1", "P2")).ToArray();

                peopleWithDifferentNames.Length.ShouldBe(2);

                peopleWithDifferentNames[0].SomeId.ShouldBe(1);
                peopleWithDifferentNames[0].SomeName.ShouldBe("P1");
                peopleWithDifferentNames[0].Age.ShouldBe(10);

                peopleWithDifferentNames[1].SomeId.ShouldBe(2);
                peopleWithDifferentNames[1].SomeName.ShouldBe("P2");
                peopleWithDifferentNames[1].Age.ShouldBe(20);

                var nonExistingPeople = (await repo.GetAsync(p => p.Age, 60)).ToArray();

                nonExistingPeople.ShouldBeEmpty();
            }
        }

        [Test] // [ToDo] - Add more filters then do the same for aliased
        public async Task When_getting_non_aliased_models_by_filter()
        {
            using (IDatabase db = new SqliteDatabase("Data Source=:memory:"))
            {
                var repo = db.GetRepository<Person>();

                await db.Connection.ExecuteAsync(TableQuery);

                (await repo.GetAsync()).ShouldBeEmpty();

                var people = new[]
                {
                    new Person { Name = "P1", Age = 10 },
                    new Person { Name = "P2", Age = 20 },
                    new Person { Id = 123, Name = "P3", Age = 30 },
                    new Person { Name = "P4", Age = 10 },
                    new Person { Name = "P5", Age = 40 },
                    new Person { Name = "P5", Age = 50 }
                };

                (await repo.InsertAsync(people)).ShouldBe(6);

                var filter1 = new QueryFilter<Person>();
                filter1.And(p => p.Age, Operand.GreaterThan, 30);
                filter1.Or(p => p.Name, Operand.Equal, "P1");

                var filter1Result = (await repo.GetAsync(filter1)).ToArray();
                filter1Result.Length.ShouldBe(3);

                filter1Result[0].Id.ShouldBe(1);
                filter1Result[0].Name.ShouldBe("P1");
                filter1Result[0].Age.ShouldBe(10);

                filter1Result[1].Id.ShouldBe(5);
                filter1Result[1].Name.ShouldBe("P5");
                filter1Result[1].Age.ShouldBe(40);

                filter1Result[2].Id.ShouldBe(6);
                filter1Result[2].Name.ShouldBe("P5");
                filter1Result[2].Age.ShouldBe(50);

                var filter2 = new QueryFilter<Person>();
                filter2.And(p => p.Age, Operand.GreaterThanOrEqual, 50);
                filter2.Or(p => p.Age, Operand.Equal, 20);

                var filter2Result = (await repo.GetAsync(filter2)).ToArray();
                filter2Result.Length.ShouldBe(2);

                filter2Result[0].Id.ShouldBe(2);
                filter2Result[0].Name.ShouldBe("P2");
                filter2Result[0].Age.ShouldBe(20);

                filter2Result[1].Id.ShouldBe(6);
                filter2Result[1].Name.ShouldBe("P5");
                filter2Result[1].Age.ShouldBe(50);
            }
        }

        [Test]
        public async Task When_inserting_single_non_aliased_model()
        {
            using (IDatabase db = new SqliteDatabase("Data Source=:memory:"))
            {
                var repo = db.GetRepository<Person>();

                await db.Connection.ExecuteAsync(TableQuery);

                (await repo.GetAsync()).ShouldBeEmpty();

                var p1 = new Person { Name = "P1", Age = 10 };
                (await repo.InsertAsync(p1)).ShouldBe(1);

                (await repo.GetAsync()).Count().ShouldBe(1);

                var insertedP1 = (await repo.GetAsync()).Single();
                insertedP1.ShouldNotBeNull();
                insertedP1.Id.ShouldBe(1);
                insertedP1.Name.ShouldBe("P1");
                insertedP1.Age.ShouldBe(10);

                var p2 = new Person { Name = "P2", Age = 20 };
                (await repo.InsertAsync(p2)).ShouldBe(2);

                (await repo.GetAsync()).Count().ShouldBe(2);

                var insertedP2 = (await repo.GetAsync(p => p.Id, 2)).Single();
                insertedP2.ShouldNotBeNull();
                insertedP2.Id.ShouldBe(2);
                insertedP2.Name.ShouldBe("P2");
                insertedP2.Age.ShouldBe(20);

                var p3 = new Person { Id = 1, Name = "P3", Age = 30 };
                (await repo.InsertAsync(p3)).ShouldBe(3);

                (await repo.GetAsync()).Count().ShouldBe(3);

                var insertedP3 = (await repo.GetAsync(p => p.Id, 3)).Single();
                insertedP3.ShouldNotBeNull();
                insertedP3.Id.ShouldBe(3);
                insertedP3.Name.ShouldBe("P3");
                insertedP3.Age.ShouldBe(30);

                var p4 = new Person { Id = 4, Name = "P4", Age = 40 };
                (await repo.InsertAsync(p4)).ShouldBe(4);

                (await repo.GetAsync()).Count().ShouldBe(4);

                var insertedP4 = (await repo.GetAsync(p => p.Id, 4)).Single();
                insertedP4.ShouldNotBeNull();
                insertedP4.Id.ShouldBe(4);
                insertedP4.Name.ShouldBe("P4");
                insertedP4.Age.ShouldBe(40);
            }
        }

        [Test]
        public async Task When_inserting_single_aliased_model()
        {
            using (IDatabase db = new SqliteDatabase("Data Source=:memory:"))
            {
                var repo = db.GetRepository<MyPerson>();

                await db.Connection.ExecuteAsync(TableQuery);

                (await repo.GetAsync()).ShouldBeEmpty();

                var p1 = new MyPerson { SomeName = "P1", Age = 10 };
                (await repo.InsertAsync(p1)).ShouldBe(1);

                (await repo.GetAsync()).Count().ShouldBe(1);

                var insertedP1 = (await repo.GetAsync()).Single();
                insertedP1.ShouldNotBeNull();
                insertedP1.SomeId.ShouldBe(1);
                insertedP1.SomeName.ShouldBe("P1");
                insertedP1.Age.ShouldBe(10);

                var p2 = new MyPerson { SomeName = "P2", Age = 20 };
                (await repo.InsertAsync(p2)).ShouldBe(2);

                (await repo.GetAsync()).Count().ShouldBe(2);

                var insertedP2 = (await repo.GetAsync(p => p.SomeId, 2)).Single();
                insertedP2.ShouldNotBeNull();
                insertedP2.SomeId.ShouldBe(2);
                insertedP2.SomeName.ShouldBe("P2");
                insertedP2.Age.ShouldBe(20);

                var p3 = new MyPerson { SomeId = 1, SomeName = "P3", Age = 30 };
                (await repo.InsertAsync(p3)).ShouldBe(3);

                (await repo.GetAsync()).Count().ShouldBe(3);

                var insertedP3 = (await repo.GetAsync(p => p.SomeId, 3)).Single();
                insertedP3.ShouldNotBeNull();
                insertedP3.SomeId.ShouldBe(3);
                insertedP3.SomeName.ShouldBe("P3");
                insertedP3.Age.ShouldBe(30);

                var p4 = new MyPerson { SomeId = 4, SomeName = "P4", Age = 40 };
                (await repo.InsertAsync(p4)).ShouldBe(4);

                (await repo.GetAsync()).Count().ShouldBe(4);

                var insertedP4 = (await repo.GetAsync(p => p.SomeId, 4)).Single();
                insertedP4.ShouldNotBeNull();
                insertedP4.SomeId.ShouldBe(4);
                insertedP4.SomeName.ShouldBe("P4");
                insertedP4.Age.ShouldBe(40);
            }
        }

        [Test]
        public async Task When_inserting_multiple_non_aliased_model()
        {
            using (IDatabase db = new SqliteDatabase("Data Source=:memory:"))
            {
                var repo = db.GetRepository<Person>();

                await db.Connection.ExecuteAsync(TableQuery);

                (await repo.GetAsync()).ShouldBeEmpty();

                var people = new[]
                {
                    new Person { Name = "P1", Age = 10 },
                    new Person { Name = "P2", Age = 20 },
                    new Person { Id = 123, Name = "P3", Age = 30 },
                    new Person { Name = "P4", Age = 40 }
                };

                (await repo.InsertAsync(people)).ShouldBe(4);

                var insertedPeople = (await repo.GetAsync()).ToArray();

                insertedPeople.Length.ShouldBe(4);

                var p1 = insertedPeople[0];
                p1.Id.ShouldBe(1);
                p1.Name.ShouldBe("P1");
                p1.Age.ShouldBe(10);

                var p2 = insertedPeople[1];
                p2.Id.ShouldBe(2);
                p2.Name.ShouldBe("P2");
                p2.Age.ShouldBe(20);

                var p3 = insertedPeople[2];
                p3.Id.ShouldBe(3);
                p3.Name.ShouldBe("P3");
                p3.Age.ShouldBe(30);

                var p4 = insertedPeople[3];
                p4.Id.ShouldBe(4);
                p4.Name.ShouldBe("P4");
                p4.Age.ShouldBe(40);
            }
        }

        [Test]
        public async Task When_inserting_multiple_aliased_model()
        {
            using (IDatabase db = new SqliteDatabase("Data Source=:memory:"))
            {
                var repo = db.GetRepository<MyPerson>();

                await db.Connection.ExecuteAsync(TableQuery);

                (await repo.GetAsync()).ShouldBeEmpty();

                var people = new[]
                {
                    new MyPerson { SomeName = "P1", Age = 10 },
                    new MyPerson { SomeName = "P2", Age = 20 },
                    new MyPerson { SomeId = 123, SomeName = "P3", Age = 30 },
                    new MyPerson { SomeName = "P4", Age = 40 }
                };

                (await repo.InsertAsync(people)).ShouldBe(4);

                var insertedPeople = (await repo.GetAsync()).ToArray();

                insertedPeople.Length.ShouldBe(4);

                var p1 = insertedPeople[0];
                p1.SomeId.ShouldBe(1);
                p1.SomeName.ShouldBe("P1");
                p1.Age.ShouldBe(10);

                var p2 = insertedPeople[1];
                p2.SomeId.ShouldBe(2);
                p2.SomeName.ShouldBe("P2");
                p2.Age.ShouldBe(20);

                var p3 = insertedPeople[2];
                p3.SomeId.ShouldBe(3);
                p3.SomeName.ShouldBe("P3");
                p3.Age.ShouldBe(30);

                var p4 = insertedPeople[3];
                p4.SomeId.ShouldBe(4);
                p4.SomeName.ShouldBe("P4");
                p4.Age.ShouldBe(40);
            }
        }

        [Test]
        public async Task When_updating_single_by_id_non_aliased_model()
        {
            using (IDatabase db = new SqliteDatabase("Data Source=:memory:"))
            {
                var repo = db.GetRepository<Person>();

                await db.Connection.ExecuteAsync(TableQuery);

                (await repo.GetAsync()).ShouldBeEmpty();

                var p1 = new Person { Name = "P1", Age = 10 };
                (await repo.InsertAsync(p1)).ShouldBe(1);

                var insertedP1 = (await repo.GetAsync()).Single();
                insertedP1.ShouldNotBeNull();
                insertedP1.Id.ShouldBe(1);
                insertedP1.Name.ShouldBe("P1");
                insertedP1.Age.ShouldBe(10);

                var p2 = new Person { Name = "P2", Age = 20 };
                (await repo.InsertAsync(p2)).ShouldBe(2);

                var insertedP2 = (await repo.GetAsync(p => p.Id, 2)).Single();
                insertedP2.ShouldNotBeNull();
                insertedP2.Id.ShouldBe(2);
                insertedP2.Name.ShouldBe("P2");
                insertedP2.Age.ShouldBe(20);

                (await repo.GetAsync()).Count().ShouldBe(2);

                var updateToP1 = insertedP1;
                updateToP1.Name = "P1-updated";
                updateToP1.Age = 60;

                (await repo.UpdateAsync(updateToP1)).ShouldBe(1);

                (await repo.GetAsync()).Count().ShouldBe(2);

                var updatedP1 = (await repo.GetAsync(p => p.Id, updateToP1.Id)).Single();
                updatedP1.ShouldNotBeNull();
                updatedP1.Id.ShouldBe(updateToP1.Id);
                updatedP1.Name.ShouldBe("P1-updated");
                updatedP1.Age.ShouldBe(60);

                var nonExistingPerson = new Person { Id = 1234, Name = "Santa", Age = 96 };
                (await repo.UpdateAsync(nonExistingPerson)).ShouldBe(0);

                (await repo.GetAsync()).Count().ShouldBe(2);
            }
        }

        [Test]
        public async Task When_updating_single_by_id_aliased_model()
        {
            using (IDatabase db = new SqliteDatabase("Data Source=:memory:"))
            {
                var repo = db.GetRepository<MyPerson>();

                await db.Connection.ExecuteAsync(TableQuery);

                (await repo.GetAsync()).ShouldBeEmpty();

                var p1 = new MyPerson { SomeName = "P1", Age = 10 };
                (await repo.InsertAsync(p1)).ShouldBe(1);

                var insertedP1 = (await repo.GetAsync()).Single();
                insertedP1.ShouldNotBeNull();
                insertedP1.SomeId.ShouldBe(1);
                insertedP1.SomeName.ShouldBe("P1");
                insertedP1.Age.ShouldBe(10);

                var p2 = new MyPerson { SomeName = "P2", Age = 20 };
                (await repo.InsertAsync(p2)).ShouldBe(2);

                var insertedP2 = (await repo.GetAsync(p => p.SomeId, 2)).Single();
                insertedP2.ShouldNotBeNull();
                insertedP2.SomeId.ShouldBe(2);
                insertedP2.SomeName.ShouldBe("P2");
                insertedP2.Age.ShouldBe(20);

                (await repo.GetAsync()).Count().ShouldBe(2);

                var updateToP1 = insertedP1;
                updateToP1.SomeName = "P1-updated";
                updateToP1.Age = 60;

                (await repo.UpdateAsync(updateToP1)).ShouldBe(1);

                (await repo.GetAsync()).Count().ShouldBe(2);

                var updatedP1 = (await repo.GetAsync(p => p.SomeId, updateToP1.SomeId)).Single();
                updatedP1.ShouldNotBeNull();
                updatedP1.SomeId.ShouldBe(updateToP1.SomeId);
                updatedP1.SomeName.ShouldBe("P1-updated");
                updatedP1.Age.ShouldBe(60);

                var nonExistingPerson = new MyPerson { SomeId = 1234, SomeName = "Santa", Age = 96 };
                (await repo.UpdateAsync(nonExistingPerson)).ShouldBe(0);

                (await repo.GetAsync()).Count().ShouldBe(2);
            }
        }

        [Test]
        public async Task When_updating_custom_non_aliased_model()
        {
            using (IDatabase db = new SqliteDatabase("Data Source=:memory:"))
            {
                var repo = db.GetRepository<Person>();

                await db.Connection.ExecuteAsync(TableQuery);

                (await repo.GetAsync()).ShouldBeEmpty();

                var people = new[]
                {
                    new Person { Name = "P1", Age = 10 },
                    new Person { Name = "P2", Age = 20 },
                    new Person { Name = "P3", Age = 30 },
                    new Person { Name = "P4", Age = 30 },
                    new Person { Name = "P5", Age = 50 },
                    new Person { Name = "P5", Age = 60 },
                    new Person { Name = "P5", Age = 70 }
                };

                (await repo.InsertAsync(people)).ShouldBe(7);

                var insertedPeople = (await repo.GetAsync()).ToArray();

                var template1 = new Person { Name = "P0", Age = 0 };
                (await repo.UpdateAsync(template1, p => p.Age, 10)).ShouldBe(1);

                var snapshot1 = (await repo.GetAsync()).ToArray();
                snapshot1.Length.ShouldBe(insertedPeople.Length);

                snapshot1[0].Id.ShouldBe(1);
                snapshot1[0].Name.ShouldBe("P0");
                snapshot1[0].Age.ShouldBe(0);

                for (var i = 1; i < insertedPeople.Length; i++)
                {
                    snapshot1[i].Id.ShouldBe(insertedPeople[i].Id);
                    snapshot1[i].Name.ShouldBe(insertedPeople[i].Name);
                    snapshot1[i].Age.ShouldBe(insertedPeople[i].Age);
                }

                var template2 = new Person { Name = "P100", Age = 100 };
                (await repo.UpdateAsync(template2, p => p.Name, "P5")).ShouldBe(3);

                var snapshot2 = (await repo.GetAsync()).ToArray();
                snapshot2.Length.ShouldBe(insertedPeople.Length);

                for (var i = snapshot1.Length - 4; i >= 0; i--)
                {
                    snapshot2[i].Id.ShouldBe(snapshot1[i].Id);
                    snapshot2[i].Name.ShouldBe(snapshot1[i].Name);
                    snapshot2[i].Age.ShouldBe(snapshot1[i].Age);
                }

                snapshot2[4].Id.ShouldBe(5);
                snapshot2[4].Name.ShouldBe("P100");
                snapshot2[4].Age.ShouldBe(100);

                snapshot2[5].Id.ShouldBe(6);
                snapshot2[5].Name.ShouldBe("P100");
                snapshot2[5].Age.ShouldBe(100);

                snapshot2[6].Id.ShouldBe(7);
                snapshot2[6].Name.ShouldBe("P100");
                snapshot2[6].Age.ShouldBe(100);

                var template3 = new Person { Name = "P200", Age = 200 };
                (await repo.UpdateAsync(template3, p => p.Id, null,  1,  2,  3)).ShouldBe(3);

                var snapshot3 = (await repo.GetAsync()).ToArray();
                snapshot3.Length.ShouldBe(insertedPeople.Length);

                snapshot3[0].Id.ShouldBe(1);
                snapshot3[0].Name.ShouldBe("P200");
                snapshot3[0].Age.ShouldBe(200);

                snapshot3[1].Id.ShouldBe(2);
                snapshot3[1].Name.ShouldBe("P200");
                snapshot3[1].Age.ShouldBe(200);

                snapshot3[2].Id.ShouldBe(3);
                snapshot3[2].Name.ShouldBe("P200");
                snapshot3[2].Age.ShouldBe(200);

                for (var i = 3; i < snapshot2.Length; i++)
                {
                    snapshot3[i].Id.ShouldBe(snapshot2[i].Id);
                    snapshot3[i].Name.ShouldBe(snapshot2[i].Name);
                    snapshot3[i].Age.ShouldBe(snapshot2[i].Age);
                }
            }
        }

        [Test]
        public async Task When_updating_custom_aliased_model()
        {
            using (IDatabase db = new SqliteDatabase("Data Source=:memory:"))
            {
                var repo = db.GetRepository<MyPerson>();

                await db.Connection.ExecuteAsync(TableQuery);

                (await repo.GetAsync()).ShouldBeEmpty();

                var people = new[]
                {
                    new MyPerson { SomeName = "P1", Age = 10 },
                    new MyPerson { SomeName = "P2", Age = 20 },
                    new MyPerson { SomeName = "P3", Age = 30 },
                    new MyPerson { SomeName = "P4", Age = 30 },
                    new MyPerson { SomeName = "P5", Age = 50 },
                    new MyPerson { SomeName = "P5", Age = 60 },
                    new MyPerson { SomeName = "P5", Age = 70 }
                };

                (await repo.InsertAsync(people)).ShouldBe(7);

                var insertedPeople = (await repo.GetAsync()).ToArray();

                var template1 = new MyPerson { SomeName = "P0", Age = 0 };
                (await repo.UpdateAsync(template1, p => p.Age, 10)).ShouldBe(1);

                var snapshot1 = (await repo.GetAsync()).ToArray();
                snapshot1.Length.ShouldBe(insertedPeople.Length);

                snapshot1[0].SomeId.ShouldBe(1);
                snapshot1[0].SomeName.ShouldBe("P0");
                snapshot1[0].Age.ShouldBe(0);

                for (var i = 1; i < insertedPeople.Length; i++)
                {
                    snapshot1[i].SomeId.ShouldBe(insertedPeople[i].SomeId);
                    snapshot1[i].SomeName.ShouldBe(insertedPeople[i].SomeName);
                    snapshot1[i].Age.ShouldBe(insertedPeople[i].Age);
                }

                var template2 = new MyPerson { SomeName = "P100", Age = 100 };
                (await repo.UpdateAsync(template2, p => p.SomeName, "P5")).ShouldBe(3);

                var snapshot2 = (await repo.GetAsync()).ToArray();
                snapshot2.Length.ShouldBe(insertedPeople.Length);

                for (var i = snapshot1.Length - 4; i >= 0; i--)
                {
                    snapshot2[i].SomeId.ShouldBe(snapshot1[i].SomeId);
                    snapshot2[i].SomeName.ShouldBe(snapshot1[i].SomeName);
                    snapshot2[i].Age.ShouldBe(snapshot1[i].Age);
                }

                snapshot2[4].SomeId.ShouldBe(5);
                snapshot2[4].SomeName.ShouldBe("P100");
                snapshot2[4].Age.ShouldBe(100);

                snapshot2[5].SomeId.ShouldBe(6);
                snapshot2[5].SomeName.ShouldBe("P100");
                snapshot2[5].Age.ShouldBe(100);

                snapshot2[6].SomeId.ShouldBe(7);
                snapshot2[6].SomeName.ShouldBe("P100");
                snapshot2[6].Age.ShouldBe(100);

                var template3 = new MyPerson { SomeName = "P200", Age = 200 };
                (await repo.UpdateAsync(template3, p => p.SomeId, null, 1, 2, 3)).ShouldBe(3);

                var snapshot3 = (await repo.GetAsync()).ToArray();
                snapshot3.Length.ShouldBe(insertedPeople.Length);

                snapshot3[0].SomeId.ShouldBe(1);
                snapshot3[0].SomeName.ShouldBe("P200");
                snapshot3[0].Age.ShouldBe(200);

                snapshot3[1].SomeId.ShouldBe(2);
                snapshot3[1].SomeName.ShouldBe("P200");
                snapshot3[1].Age.ShouldBe(200);

                snapshot3[2].SomeId.ShouldBe(3);
                snapshot3[2].SomeName.ShouldBe("P200");
                snapshot3[2].Age.ShouldBe(200);

                for (var i = 3; i < snapshot2.Length; i++)
                {
                    snapshot3[i].SomeId.ShouldBe(snapshot2[i].SomeId);
                    snapshot3[i].SomeName.ShouldBe(snapshot2[i].SomeName);
                    snapshot3[i].Age.ShouldBe(snapshot2[i].Age);
                }
            }
        }

        [Test]
        public async Task When_updating_multiple_non_aliased_model()
        {
            using (IDatabase db = new SqliteDatabase("Data Source=:memory:"))
            {
                var repo = db.GetRepository<Person>();

                await db.Connection.ExecuteAsync(TableQuery);

                (await repo.GetAsync()).ShouldBeEmpty();

                var people = new[]
                {
                    new Person { Name = "P1", Age = 10 },
                    new Person { Name = "P2", Age = 20 },
                    new Person { Id = 123, Name = "P3", Age = 30 },
                    new Person { Name = "P4", Age = 40 }
                };

                (await repo.InsertAsync(people)).ShouldBe(4);

                var insertedPeople = (await repo.GetAsync()).ToArray();

                var someOtherPerson = new Person { Name = "Santa", Age = 96 };
                (await repo.InsertAsync(someOtherPerson)).ShouldBe(5);

                (await repo.GetAsync()).Count().ShouldBe(people.Length + 1);

                insertedPeople.Length.ShouldBe(4);

                // update the people
                Array.ForEach(insertedPeople, p =>
                {
                    p.Name = p.Name + "-updated";
                    p.Age = p.Age * 2;
                });

                (await repo.UpdateAsync(insertedPeople)).ShouldBe(insertedPeople.Length);

                (await repo.GetAsync()).Count().ShouldBe(people.Length + 1);

                var allPeople = (await repo.GetAsync()).OrderBy(p => p.Id).ToArray();

                var p1 = allPeople[0];
                p1.Id.ShouldBe(1);
                p1.Name.ShouldBe("P1-updated");
                p1.Age.ShouldBe(20);

                var p2 = allPeople[1];
                p2.Id.ShouldBe(2);
                p2.Name.ShouldBe("P2-updated");
                p2.Age.ShouldBe(40);

                var p3 = allPeople[2];
                p3.Id.ShouldBe(3);
                p3.Name.ShouldBe("P3-updated");
                p3.Age.ShouldBe(60);

                var p4 = allPeople[3];
                p4.Id.ShouldBe(4);
                p4.Name.ShouldBe("P4-updated");
                p4.Age.ShouldBe(80);

                var p5 = allPeople[4];
                p5.Id.ShouldBe(5);
                p5.Name.ShouldBe(someOtherPerson.Name);
                p5.Age.ShouldBe(someOtherPerson.Age);
            }
        }

        [Test]
        public async Task When_updating_multiple_aliased_model()
        {
            using (IDatabase db = new SqliteDatabase("Data Source=:memory:"))
            {
                var repo = db.GetRepository<MyPerson>();

                await db.Connection.ExecuteAsync(TableQuery);

                (await repo.GetAsync()).ShouldBeEmpty();

                var people = new[]
                {
                    new MyPerson { SomeName = "P1", Age = 10 },
                    new MyPerson { SomeName = "P2", Age = 20 },
                    new MyPerson { SomeId = 123, SomeName = "P3", Age = 30 },
                    new MyPerson { SomeName = "P4", Age = 40 }
                };

                (await repo.InsertAsync(people)).ShouldBe(4);

                var insertedPeople = (await repo.GetAsync()).ToArray();

                var someOtherPerson = new MyPerson { SomeName = "Santa", Age = 96 };
                (await repo.InsertAsync(someOtherPerson)).ShouldBe(5);

                (await repo.GetAsync()).Count().ShouldBe(people.Length + 1);

                insertedPeople.Length.ShouldBe(4);

                // update the people
                Array.ForEach(insertedPeople, p =>
                {
                    p.SomeName = p.SomeName + "-updated";
                    p.Age = p.Age * 2;
                });

                (await repo.UpdateAsync(insertedPeople)).ShouldBe(insertedPeople.Length);

                (await repo.GetAsync()).Count().ShouldBe(people.Length + 1);

                var allPeople = (await repo.GetAsync()).OrderBy(p => p.SomeId).ToArray();

                var p1 = allPeople[0];
                p1.SomeId.ShouldBe(1);
                p1.SomeName.ShouldBe("P1-updated");
                p1.Age.ShouldBe(20);

                var p2 = allPeople[1];
                p2.SomeId.ShouldBe(2);
                p2.SomeName.ShouldBe("P2-updated");
                p2.Age.ShouldBe(40);

                var p3 = allPeople[2];
                p3.SomeId.ShouldBe(3);
                p3.SomeName.ShouldBe("P3-updated");
                p3.Age.ShouldBe(60);

                var p4 = allPeople[3];
                p4.SomeId.ShouldBe(4);
                p4.SomeName.ShouldBe("P4-updated");
                p4.Age.ShouldBe(80);

                var p5 = allPeople[4];
                p5.SomeId.ShouldBe(5);
                p5.SomeName.ShouldBe(someOtherPerson.SomeName);
                p5.Age.ShouldBe(someOtherPerson.Age);
            }
        }

        [Test]
        public async Task When_deleting_non_aliased_model()
        {
            using (IDatabase db = new SqliteDatabase("Data Source=:memory:"))
            {
                var repo = db.GetRepository<Person>();

                await db.Connection.ExecuteAsync(TableQuery);

                (await repo.GetAsync()).ShouldBeEmpty();

                var people = new[]
                {
                    new Person { Name = "P1", Age = 10 },
                    new Person { Name = "P2", Age = 20 },
                    new Person { Id = 123, Name = "P3", Age = 30 },
                    new Person { Name = "P4", Age = 30 }
                };

                (await repo.InsertAsync(people)).ShouldBe(4);

                var insertedPeople = (await repo.GetAsync()).ToArray();
                insertedPeople.Length.ShouldBe(people.Length);

                (await repo.GetAsync(p => p.Id, 1)).ShouldNotBeEmpty();
                (await repo.DeleteAsync(p => p.Id, 1)).ShouldBe(1);
                (await repo.GetAsync()).Count().ShouldBe(3);
                (await repo.GetAsync(p => p.Id, 1)).ShouldBeEmpty();

                (await repo.GetAsync(p => p.Age, 30)).Count().ShouldBe(2);
                (await repo.DeleteAsync(p => p.Age, 30)).ShouldBe(2);
                (await repo.GetAsync()).Count().ShouldBe(1);
                (await repo.GetAsync(p => p.Age, 30)).ShouldBeEmpty();

                var remainingPeople = (await repo.GetAsync()).Single();
                remainingPeople.Id.ShouldBe(2);
                remainingPeople.Name.ShouldBe("P2");
                remainingPeople.Age.ShouldBe(20);

                var morePeople = new[]
                {
                    new Person { Name = "MP1", Age = 50 },
                    new Person { Name = "MP2", Age = 60 },
                    new Person { Name = "MP3", Age = 70 },
                    new Person { Name = "MP4", Age = 80 },
                    new Person { Name = "MP5", Age = 90 },
                    new Person { Name = "MP5", Age = 100 }
                };

                (await repo.InsertAsync(morePeople)).ShouldBe(6);

                var allPeople = (await repo.GetAsync()).ToArray();
                allPeople.Length.ShouldBe(morePeople.Length + 1);

                allPeople[0].Id.ShouldBe(remainingPeople.Id);
                allPeople[0].Name.ShouldBe(remainingPeople.Name);
                allPeople[0].Age.ShouldBe(remainingPeople.Age);
                (await repo.DeleteAsync(p => p.Id, null, remainingPeople.Id, allPeople[1].Id)).ShouldBe(2);

                (await repo.GetAsync()).Count().ShouldBe(morePeople.Length + 1 - 2);

                (await repo.DeleteAsync(p => p.Name, null, "MP4", "MP5")).ShouldBe(3);

                var totalPeopleRemaining = (await repo.GetAsync()).ToArray();
                totalPeopleRemaining.Length.ShouldBe(2);

                totalPeopleRemaining[0].Id.ShouldBe(allPeople[2].Id);
                totalPeopleRemaining[0].Name.ShouldBe(allPeople[2].Name);
                totalPeopleRemaining[0].Age.ShouldBe(allPeople[2].Age);

                totalPeopleRemaining[1].Id.ShouldBe(allPeople[3].Id);
                totalPeopleRemaining[1].Name.ShouldBe(allPeople[3].Name);
                totalPeopleRemaining[1].Age.ShouldBe(allPeople[3].Age);
            }
        }

        [Test]
        public async Task When_deleting_aliased_model()
        {
            using (IDatabase db = new SqliteDatabase("Data Source=:memory:"))
            {
                var repo = db.GetRepository<MyPerson>();

                await db.Connection.ExecuteAsync(TableQuery);

                (await repo.GetAsync()).ShouldBeEmpty();

                var people = new[]
                {
                    new MyPerson { SomeName = "P1", Age = 10 },
                    new MyPerson { SomeName = "P2", Age = 20 },
                    new MyPerson { SomeId = 123, SomeName = "P3", Age = 30 },
                    new MyPerson { SomeName = "P4", Age = 30 }
                };

                (await repo.InsertAsync(people)).ShouldBe(4);

                var insertedPeople = (await repo.GetAsync()).ToArray();
                insertedPeople.Length.ShouldBe(people.Length);

                (await repo.GetAsync(p => p.SomeId, 1)).ShouldNotBeEmpty();
                (await repo.DeleteAsync(p => p.SomeId, 1)).ShouldBe(1);
                (await repo.GetAsync()).Count().ShouldBe(3);
                (await repo.GetAsync(p => p.SomeId, 1)).ShouldBeEmpty();

                (await repo.GetAsync(p => p.Age, 30)).Count().ShouldBe(2);
                (await repo.DeleteAsync(p => p.Age, 30)).ShouldBe(2);
                (await repo.GetAsync()).Count().ShouldBe(1);
                (await repo.GetAsync(p => p.Age, 30)).ShouldBeEmpty();

                var remainingPeople = (await repo.GetAsync()).Single();
                remainingPeople.SomeId.ShouldBe(2);
                remainingPeople.SomeName.ShouldBe("P2");
                remainingPeople.Age.ShouldBe(20);

                var morePeople = new[]
                {
                    new MyPerson { SomeName = "MP1", Age = 50 },
                    new MyPerson { SomeName = "MP2", Age = 60 },
                    new MyPerson { SomeName = "MP3", Age = 70 },
                    new MyPerson { SomeName = "MP4", Age = 80 },
                    new MyPerson { SomeName = "MP5", Age = 90 },
                    new MyPerson { SomeName = "MP5", Age = 100 }
                };

                (await repo.InsertAsync(morePeople)).ShouldBe(6);

                var allPeople = (await repo.GetAsync()).ToArray();
                allPeople.Length.ShouldBe(morePeople.Length + 1);

                allPeople[0].SomeId.ShouldBe(remainingPeople.SomeId);
                allPeople[0].SomeName.ShouldBe(remainingPeople.SomeName);
                allPeople[0].Age.ShouldBe(remainingPeople.Age);
                (await repo.DeleteAsync(p => p.SomeId, null, remainingPeople.SomeId, allPeople[1].SomeId)).ShouldBe(2);

                (await repo.GetAsync()).Count().ShouldBe(morePeople.Length + 1 - 2);

                (await repo.DeleteAsync(p => p.SomeName, null, "MP4", "MP5")).ShouldBe(3);

                var totalPeopleRemaining = (await repo.GetAsync()).ToArray();
                totalPeopleRemaining.Length.ShouldBe(2);

                totalPeopleRemaining[0].SomeId.ShouldBe(allPeople[2].SomeId);
                totalPeopleRemaining[0].SomeName.ShouldBe(allPeople[2].SomeName);
                totalPeopleRemaining[0].Age.ShouldBe(allPeople[2].Age);

                totalPeopleRemaining[1].SomeId.ShouldBe(allPeople[3].SomeId);
                totalPeopleRemaining[1].SomeName.ShouldBe(allPeople[3].SomeName);
                totalPeopleRemaining[1].Age.ShouldBe(allPeople[3].Age);
            }
        }

        [Test]
        public async Task When_deleting_all_non_aliased_model()
        {
            using (IDatabase db = new SqliteDatabase("Data Source=:memory:"))
            {
                var repo = db.GetRepository<Person>();

                await db.Connection.ExecuteAsync(TableQuery);
                
                (await repo.GetAsync()).ShouldBeEmpty();
                
                (await repo.DeleteAllAsync()).ShouldBe(0);

                var people = new[]
                {
                    new Person { Name = "P1", Age = 10 },
                    new Person { Name = "P2", Age = 20 },
                    new Person { Id = 123, Name = "P3", Age = 30 },
                    new Person { Name = "P4", Age = 30 }
                };

                (await repo.InsertAsync(people)).ShouldBe(4);

                var insertedPeople = (await repo.GetAsync()).ToArray();
                insertedPeople.Length.ShouldBe(people.Length);

                (await repo.DeleteAllAsync()).ShouldBe(insertedPeople.Length);
                (await repo.GetAsync()).ShouldBeEmpty();
            }
        }

        [Test]
        public async Task When_deleting_all_aliased_model()
        {
            using (IDatabase db = new SqliteDatabase("Data Source=:memory:"))
            {
                var repo = db.GetRepository<MyPerson>();

                await db.Connection.ExecuteAsync(TableQuery);

                (await repo.GetAsync()).ShouldBeEmpty();

                (await repo.DeleteAllAsync()).ShouldBe(0);

                var people = new[]
                {
                    new MyPerson { SomeName = "P1", Age = 10 },
                    new MyPerson { SomeName = "P2", Age = 20 },
                    new MyPerson { SomeId = 123, SomeName = "P3", Age = 30 },
                    new MyPerson { SomeName = "P4", Age = 30 }
                };

                (await repo.InsertAsync(people)).ShouldBe(4);

                var insertedPeople = (await repo.GetAsync()).ToArray();
                insertedPeople.Length.ShouldBe(people.Length);

                (await repo.DeleteAllAsync()).ShouldBe(insertedPeople.Length);
                (await repo.GetAsync()).ShouldBeEmpty();
            }
        }

        [Test]
        public async Task When_counting_non_aliased_model()
        {
            using (IDatabase db = new SqliteDatabase("Data Source=:memory:"))
            {
                var repo = db.GetRepository<Person>();

                await db.Connection.ExecuteAsync(TableQuery);

                (await repo.CountAsync(p => p.Id)).ShouldBe((ulong)0);
                (await repo.CountAsync(p => p.Age)).ShouldBe((ulong)0);
                (await repo.CountAsync(p => p.Name)).ShouldBe((ulong)0);
                
                (await repo.CountAsync(p => p.Id, true)).ShouldBe((ulong)0);
                (await repo.CountAsync(p => p.Age, true)).ShouldBe((ulong)0);
                (await repo.CountAsync(p => p.Name, true)).ShouldBe((ulong)0);

                var people = new[]
                {
                    new Person { Name = "P1", Age = 10 },
                    new Person { Name = "P2", Age = 30 },
                    new Person { Id = 123, Name = "P3", Age = 30 },
                    new Person { Name = "P3", Age = 30 }
                };

                (await repo.InsertAsync(people)).ShouldBe(4);

                (await repo.CountAsync(p => p.Id)).ShouldBe((ulong)people.Length);
                (await repo.CountAsync(p => p.Age)).ShouldBe((ulong)people.Length);
                (await repo.CountAsync(p => p.Name)).ShouldBe((ulong)people.Length);
                
                (await repo.CountAsync(p => p.Id, true)).ShouldBe((ulong)people.Length);
                (await repo.CountAsync(p => p.Age, true)).ShouldBe((ulong)2);
                (await repo.CountAsync(p => p.Name, true)).ShouldBe((ulong)3);
                
                await repo.DeleteAllAsync();
                
                (await repo.CountAsync(p => p.Id)).ShouldBe((ulong)0);
                (await repo.CountAsync(p => p.Age)).ShouldBe((ulong)0);
                (await repo.CountAsync(p => p.Name)).ShouldBe((ulong)0);
                
                (await repo.CountAsync(p => p.Id, true)).ShouldBe((ulong)0);
                (await repo.CountAsync(p => p.Age, true)).ShouldBe((ulong)0);
                (await repo.CountAsync(p => p.Name, true)).ShouldBe((ulong)0);
            }
        }

        [Test]
        public async Task When_counting_aliased_model()
        {
            using (IDatabase db = new SqliteDatabase("Data Source=:memory:"))
            {
                var repo = db.GetRepository<MyPerson>();

                await db.Connection.ExecuteAsync(TableQuery);
                
                (await repo.CountAsync(p => p.SomeId)).ShouldBe((ulong)0);
                (await repo.CountAsync(p => p.Age)).ShouldBe((ulong)0);
                (await repo.CountAsync(p => p.SomeName)).ShouldBe((ulong)0);
                
                (await repo.CountAsync(p => p.SomeId, true)).ShouldBe((ulong)0);
                (await repo.CountAsync(p => p.Age, true)).ShouldBe((ulong)0);
                (await repo.CountAsync(p => p.SomeName, true)).ShouldBe((ulong)0);

                var people = new[]
                {
                    new MyPerson { SomeName = "P1", Age = 10 },
                    new MyPerson { SomeName = "P2", Age = 30 },
                    new MyPerson { SomeId = 123, SomeName = "P3", Age = 30 },
                    new MyPerson { SomeName = "P3", Age = 30 }
                };

                (await repo.InsertAsync(people)).ShouldBe(4);

                (await repo.CountAsync(p => p.SomeId)).ShouldBe((ulong)people.Length);
                (await repo.CountAsync(p => p.Age)).ShouldBe((ulong)people.Length);
                (await repo.CountAsync(p => p.SomeName)).ShouldBe((ulong)people.Length);
                
                (await repo.CountAsync(p => p.SomeId, true)).ShouldBe((ulong)people.Length);
                (await repo.CountAsync(p => p.Age, true)).ShouldBe((ulong)2);
                (await repo.CountAsync(p => p.SomeName, true)).ShouldBe((ulong)3);
                
                await repo.DeleteAllAsync();
                
                (await repo.CountAsync(p => p.SomeId)).ShouldBe((ulong)0);
                (await repo.CountAsync(p => p.Age)).ShouldBe((ulong)0);
                (await repo.CountAsync(p => p.SomeName)).ShouldBe((ulong)0);
                
                (await repo.CountAsync(p => p.SomeId, true)).ShouldBe((ulong)0);
                (await repo.CountAsync(p => p.Age, true)).ShouldBe((ulong)0);
                (await repo.CountAsync(p => p.SomeName, true)).ShouldBe((ulong)0);
            }
        }

        [Test]
        public async Task When_min_non_aliased_model()
        {
            using (IDatabase db = new SqliteDatabase("Data Source=:memory:"))
            {
                var repo = db.GetRepository<Person>();

                await db.Connection.ExecuteAsync(TableQuery);

                (await repo.MinAsync(p => p.Id)).ShouldBe(0);
                (await repo.MinAsync(p => p.Age)).ShouldBe(0);
                (await repo.MinAsync(p => p.Name)).ShouldBeNull();

                var people = new[]
                {
                    new Person { Name = "P1", Age = 10 },
                    new Person { Name = "P2", Age = 30 },
                    new Person { Id = 123, Name = "P3", Age = 30 },
                    new Person { Name = "P3", Age = 30 }
                };

                (await repo.InsertAsync(people)).ShouldBe(4);

                (await repo.MinAsync(p => p.Id)).ShouldBe(1);
                (await repo.MinAsync(p => p.Age)).ShouldBe(10);
                (await repo.MinAsync(p => p.Name)).ShouldBe("P1");
            }
        }

        [Test]
        public async Task When_min_aliased_model()
        {
            using (IDatabase db = new SqliteDatabase("Data Source=:memory:"))
            {
                var repo = db.GetRepository<MyPerson>();

                await db.Connection.ExecuteAsync(TableQuery);

                (await repo.MinAsync(p => p.SomeId)).ShouldBe(0);
                (await repo.MinAsync(p => p.Age)).ShouldBe(0);
                (await repo.MinAsync(p => p.SomeName)).ShouldBeNull();

                var people = new[]
                {
                    new MyPerson { SomeName = "P1", Age = 10 },
                    new MyPerson { SomeName = "P2", Age = 30 },
                    new MyPerson { SomeId = 123, SomeName = "P3", Age = 30 },
                    new MyPerson { SomeName = "P3", Age = 30 }
                };

                (await repo.InsertAsync(people)).ShouldBe(4);

                (await repo.MinAsync(p => p.SomeId)).ShouldBe(1);
                (await repo.MinAsync(p => p.Age)).ShouldBe(10);
                (await repo.MinAsync(p => p.SomeName)).ShouldBe("P1");
            }
        }

        [Test]
        public async Task When_max_non_aliased_model()
        {
            using (IDatabase db = new SqliteDatabase("Data Source=:memory:"))
            {
                var repo = db.GetRepository<Person>();

                await db.Connection.ExecuteAsync(TableQuery);

                (await repo.MaxAsync(p => p.Id)).ShouldBe(0);
                (await repo.MaxAsync(p => p.Age)).ShouldBe(0);
                (await repo.MaxAsync(p => p.Name)).ShouldBeNull();

                var people = new[]
                {
                    new Person { Name = "P1", Age = 10 },
                    new Person { Name = "P2", Age = 30 },
                    new Person { Id = 123, Name = "P3", Age = 30 },
                    new Person { Name = "P3", Age = 30 }
                };

                (await repo.InsertAsync(people)).ShouldBe(4);

                (await repo.MaxAsync(p => p.Id)).ShouldBe(4);
                (await repo.MaxAsync(p => p.Age)).ShouldBe(30);
                (await repo.MaxAsync(p => p.Name)).ShouldBe("P3");
            }
        }

        [Test]
        public async Task When_max_aliased_model()
        {
            using (IDatabase db = new SqliteDatabase("Data Source=:memory:"))
            {
                var repo = db.GetRepository<MyPerson>();

                await db.Connection.ExecuteAsync(TableQuery);

                (await repo.MaxAsync(p => p.SomeId)).ShouldBe(0);
                (await repo.MaxAsync(p => p.Age)).ShouldBe(0);
                (await repo.MaxAsync(p => p.SomeName)).ShouldBeNull();

                var people = new[]
                {
                    new MyPerson { SomeName = "P1", Age = 10 },
                    new MyPerson { SomeName = "P2", Age = 30 },
                    new MyPerson { SomeId = 123, SomeName = "P3", Age = 30 },
                    new MyPerson { SomeName = "P3", Age = 30 }
                };

                (await repo.InsertAsync(people)).ShouldBe(4);

                (await repo.MaxAsync(p => p.SomeId)).ShouldBe(4);
                (await repo.MaxAsync(p => p.Age)).ShouldBe(30);
                (await repo.MaxAsync(p => p.SomeName)).ShouldBe("P3");
            }
        }

        [Test]
        public async Task When_sum_non_aliased_model()
        {
            using (IDatabase db = new SqliteDatabase("Data Source=:memory:"))
            {
                var repo = db.GetRepository<Person>();

                await db.Connection.ExecuteAsync(TableQuery);

                (await repo.GetAsync()).ShouldBeEmpty();

                var people = new[]
                {
                    new Person { Name = "P1", Age = 10 },
                    new Person { Name = "P2", Age = 20 },
                    new Person { Name = "P3", Age = 30 },
                    new Person { Name = "P4", Age = 40 },
                    new Person { Name = "P5", Age = 10 }
                };

                (await repo.InsertAsync(people)).ShouldBe(5);

                var insertedPeople = (await repo.GetAsync()).ToArray();

                insertedPeople.Length.ShouldBe(5);

                (await repo.SumAsync(p => p.Id)).ShouldBe(15);
                (await repo.SumAsync(p => p.Name)).ShouldBe(0);
                (await repo.SumAsync(p => p.Age)).ShouldBe(110);
                
                (await repo.SumAsync(p => p.Id, true)).ShouldBe(15);
                (await repo.SumAsync(p => p.Name, true)).ShouldBe(0);
                (await repo.SumAsync(p => p.Age, true)).ShouldBe(100);
            }
        }

        [Test]
        public async Task When_sum_aliased_model()
        {
            using (IDatabase db = new SqliteDatabase("Data Source=:memory:"))
            {
                var repo = db.GetRepository<MyPerson>();

                await db.Connection.ExecuteAsync(TableQuery);

                (await repo.GetAsync()).ShouldBeEmpty();

                var people = new[]
                {
                    new MyPerson { SomeName = "P1", Age = 10 },
                    new MyPerson { SomeName = "P2", Age = 20 },
                    new MyPerson { SomeName = "P3", Age = 30 },
                    new MyPerson { SomeName = "P4", Age = 40 },
                    new MyPerson { SomeName = "P5", Age = 10 }
                };

                (await repo.InsertAsync(people)).ShouldBe(5);

                var insertedPeople = (await repo.GetAsync()).ToArray();

                insertedPeople.Length.ShouldBe(5);

                (await repo.SumAsync(p => p.SomeId)).ShouldBe(15);
                (await repo.SumAsync(p => p.SomeName)).ShouldBe(0);
                (await repo.SumAsync(p => p.Age)).ShouldBe(110);
                
                (await repo.SumAsync(p => p.SomeId, true)).ShouldBe(15);
                (await repo.SumAsync(p => p.SomeName, true)).ShouldBe(0);
                (await repo.SumAsync(p => p.Age, true)).ShouldBe(100);
            }
        }

        [Test]
        public async Task When_avg_non_aliased_model()
        {
            using (IDatabase db = new SqliteDatabase("Data Source=:memory:"))
            {
                var repo = db.GetRepository<Person>();

                await db.Connection.ExecuteAsync(TableQuery);

                (await repo.GetAsync()).ShouldBeEmpty();

                var people = new[]
                {
                    new Person { Name = "P1", Age = 10 },
                    new Person { Name = "P2", Age = 20 },
                    new Person { Name = "P3", Age = 30 },
                    new Person { Name = "P4", Age = 40 },
                    new Person { Name = "P5", Age = 50 },
                    new Person { Name = "P6", Age = 60 },
                    new Person { Name = "P7", Age = 10 }
                };

                (await repo.InsertAsync(people)).ShouldBe(7);

                var insertedPeople = (await repo.GetAsync()).ToArray();

                insertedPeople.Length.ShouldBe(7);

                (await repo.SumAsync(p=> p.Id)).ShouldBe(28);
                (await repo.SumAsync(p=> p.Age)).ShouldBe(220);
                
                (await repo.AvgAsync(p => p.Id)).ShouldBe(4m);
                (await repo.AvgAsync(p => p.Name)).ShouldBe(0);
                (await repo.AvgAsync(p => p.Age)).ShouldBe(31.42m, 0.01m);
                
                (await repo.AvgAsync(p => p.Id, true)).ShouldBe(4m);
                (await repo.AvgAsync(p => p.Name, true)).ShouldBe(0);
                (await repo.AvgAsync(p => p.Age, true)).ShouldBe(35m);
            }
        }

        [Test]
        public async Task When_avg_aliased_model()
        {
            using (IDatabase db = new SqliteDatabase("Data Source=:memory:"))
            {
                var repo = db.GetRepository<MyPerson>();

                await db.Connection.ExecuteAsync(TableQuery);

                (await repo.GetAsync()).ShouldBeEmpty();

                var people = new[]
                {
                    new MyPerson { SomeName = "P1", Age = 10 },
                    new MyPerson { SomeName = "P2", Age = 20 },
                    new MyPerson { SomeName = "P3", Age = 30 },
                    new MyPerson { SomeName = "P4", Age = 40 },
                    new MyPerson { SomeName = "P5", Age = 50 },
                    new MyPerson { SomeName = "P6", Age = 60 },
                    new MyPerson { SomeName = "P7", Age = 10 }
                };

                (await repo.InsertAsync(people)).ShouldBe(7);

                var insertedPeople = (await repo.GetAsync()).ToArray();

                insertedPeople.Length.ShouldBe(7);

                (await repo.SumAsync(p => p.SomeId)).ShouldBe(28);
                (await repo.SumAsync(p => p.Age)).ShouldBe(220);
                
                (await repo.AvgAsync(p => p.SomeId)).ShouldBe(4m);
                (await repo.AvgAsync(p => p.SomeName)).ShouldBe(0);
                (await repo.AvgAsync(p => p.Age)).ShouldBe(31.42m, 0.01m);
                
                (await repo.AvgAsync(p => p.SomeId, true)).ShouldBe(4m);
                (await repo.AvgAsync(p => p.SomeName, true)).ShouldBe(0);
                (await repo.AvgAsync(p => p.Age, true)).ShouldBe(35m);
            }
        }

        [Test]
        public async Task When_doing_multiple_operations_with_sample_model()
        {
            var tableQuery = SqliteSqlGenerator.Table<SampleModel>();

            using (IDatabase db = new SqliteDatabase("Data Source=:memory:;binaryguid=False;"))
            {
                (await db.ExistsAsync<SampleModel>()).ShouldBeFalse();
                await db.Connection.ExecuteAsync(tableQuery);
                (await db.ExistsAsync<SampleModel>()).ShouldBeTrue();

                var sample1 = new SampleModel
                {
                    Text = "Model-1",
                    Int = 1,
                    Decimal = 1.1234m,
                    Double = 1.51,
                    Float = 1.9f,
                    Flag = true,
                    Binary = Encoding.UTF8.GetBytes("Hidden Message!"),
                    Guid = Guid.NewGuid(),
                    DateTime = new DateTime(1912, 1, 28, 10, 35, 11, 12),
                    DateTimeOffset = new DateTimeOffset(new DateTime(1912, 1, 28, 10, 35, 11, 12), TimeSpan.FromHours(2)),
                    Composite = null
                };

                var repo = db.GetRepository<SampleModel>();

                (await repo.InsertAsync(sample1)).ShouldBe(1);

                var insertedSample1 = (await repo.GetAsync()).Single();
                insertedSample1.Id.ShouldBe(1);
                insertedSample1.Int.ShouldBe(sample1.Int);
                insertedSample1.Decimal.ShouldBe(sample1.Decimal);
                insertedSample1.Double.ShouldBe(sample1.Double);
                insertedSample1.Float.ShouldBe(sample1.Float);
                insertedSample1.Flag.ShouldBe(sample1.Flag);
                insertedSample1.Binary.ShouldBe(sample1.Binary);
                insertedSample1.Guid.ShouldBe(sample1.Guid);
                insertedSample1.DateTime.ShouldBe(new DateTime(1912, 1, 28, 10, 35, 11));

                insertedSample1.DateTimeOffset.Offset.ShouldBe(sample1.DateTimeOffset.Offset);
                insertedSample1.DateTimeOffset.DateTime.Kind.ShouldBe(sample1.DateTime.Kind);
                insertedSample1.DateTimeOffset.Date.ShouldBe(sample1.DateTimeOffset.Date);
                insertedSample1.DateTimeOffset.TimeOfDay.Hours.ShouldBe(sample1.DateTimeOffset.TimeOfDay.Hours);
                insertedSample1.DateTimeOffset.TimeOfDay.Minutes.ShouldBe(sample1.DateTimeOffset.TimeOfDay.Minutes);
                insertedSample1.DateTimeOffset.TimeOfDay.Seconds.ShouldBe(sample1.DateTimeOffset.TimeOfDay.Seconds);
                // [ToDo] - Make sure the bug is fixed in dapper - you can 
                // then use SetValue of the DateTimeOffsetHandler to store as file time or otherwise.
                // insertedSample1.DateTimeOffset.TimeOfDay.Milliseconds.ShouldBe(sample1.DateTimeOffset.TimeOfDay.Milliseconds);
                    
                insertedSample1.Composite.ShouldBe(sample1.Composite);

                insertedSample1.Flag = true;
                insertedSample1.Double = 5.6;
                insertedSample1.Text = "Updated";

                (await repo.UpdateAsync(insertedSample1)).ShouldBe(1);
                (await repo.CountAsync(p => p.Id)).ShouldBe((uint)1);
                var updatedSample1 = (await repo.GetAsync()).Single();

                updatedSample1.Id.ShouldBe(insertedSample1.Id);
                updatedSample1.Int.ShouldBe(insertedSample1.Int);
                updatedSample1.Decimal.ShouldBe(insertedSample1.Decimal);
                updatedSample1.Double.ShouldBe(insertedSample1.Double);
                updatedSample1.Float.ShouldBe(insertedSample1.Float);
                updatedSample1.Flag.ShouldBe(insertedSample1.Flag);
                updatedSample1.Binary.ShouldBe(insertedSample1.Binary);
                updatedSample1.Guid.ShouldBe(insertedSample1.Guid);
                updatedSample1.DateTime.ShouldBe(insertedSample1.DateTime);

                updatedSample1.DateTimeOffset.Offset.ShouldBe(insertedSample1.DateTimeOffset.Offset);
                updatedSample1.DateTimeOffset.DateTime.Kind.ShouldBe(insertedSample1.DateTime.Kind);
                updatedSample1.DateTimeOffset.Date.ShouldBe(insertedSample1.DateTimeOffset.Date);
                updatedSample1.DateTimeOffset.TimeOfDay.Hours.ShouldBe(insertedSample1.DateTimeOffset.TimeOfDay.Hours);
                updatedSample1.DateTimeOffset.TimeOfDay.Minutes.ShouldBe(insertedSample1.DateTimeOffset.TimeOfDay.Minutes);
                updatedSample1.DateTimeOffset.TimeOfDay.Seconds.ShouldBe(insertedSample1.DateTimeOffset.TimeOfDay.Seconds);
            }
        }
    }
}