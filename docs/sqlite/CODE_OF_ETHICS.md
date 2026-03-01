Code Of Ethics
Small. Fast. Reliable.Choose any three.
Home
Menu
About
Documentation
Download
License
Support
Purchase
Search
About
Documentation
Download
Support
Purchase
Search Documentation
Search Changelog
function toggle_div(nm) {
var w = document.getElementById(nm);
if( w.style.display=="block" ){
w.style.display = "none";
}else{
w.style.display = "block";
}
}
function toggle_search() {
var w = document.getElementById("searchmenu");
if( w.style.display=="block" ){
w.style.display = "none";
} else {
w.style.display = "block";
setTimeout(function(){
document.getElementById("searchbox").focus()
}, 30);
}
}
function div_off(nm){document.getElementById(nm).style.display="none";}
window.onbeforeunload = function(e){div_off("submenu");}
/* Disable the Search feature if we are not operating from CGI, since */
/* Search is accomplished using CGI and will not work without it. */
if( !location.origin || !location.origin.match || !location.origin.match(/http/) ){
document.getElementById("search_menubutton").style.display = "none";
}
/* Used by the Hide/Show button beside syntax diagrams, to toggle the */
function hideorshow(btn,obj){
var x = document.getElementById(obj);
var b = document.getElementById(btn);
if( x.style.display!='none' ){
x.style.display = 'none';
b.innerHTML='show';
}else{
x.style.display = '';
b.innerHTML='hide';
}
return false;
}
var antiRobot = 0;
function antiRobotGo(){
if( antiRobot!=3 ) return;
antiRobot = 7;
var j = document.getElementById("mtimelink");
if(j && j.hasAttribute("data-href")) j.href=j.getAttribute("data-href");
}
function antiRobotDefense(){
document.body.onmousedown=function(){
antiRobot |= 2;
antiRobotGo();
document.body.onmousedown=null;
}
document.body.onmousemove=function(){
antiRobot |= 2;
antiRobotGo();
document.body.onmousemove=null;
}
setTimeout(function(){
antiRobot |= 1;
antiRobotGo();
}, 100)
antiRobotGo();
}
antiRobotDefense();
Code Of Ethics
1. History
This document was originally called a "Code of Conduct" and
was created for the purpose of filling in a box on "supplier registration"
forms submitted to the SQLite developers by some clients.  However,
we subsequently learned that "Code of Conduct" has a very specific and
almost sacred meaning to some readers, a meaning to which this
document does not conform
[1][2][3].
Therefore this document was renamed to "Code of Ethics", as
we are encouraged to do by rule 71 in particular and also rules 2, 8, 9, 18, 19,
30, 66, and in the spirit of all the rest.
This document continues to be used for its original purpose - providing
a reference to fill in the "code of conduct" box on supplier registration
forms.
2. Purpose
The founder of SQLite, and all of the current developers at the time
when this document was composed, have pledged to govern their
interactions with each other, with their clients,
and with the larger SQLite user community in
accordance with the "instruments of good works" from chapter 4 of
The Rule of St. Benedict
(hereafter: "The Rule").
This code of ethics has proven its mettle in thousands of diverse
communities for over 1,500 years, and has served as a baseline for many
civil law codes since the time of Charlemagne.
2.1. Scope of Application
No one is required to follow The Rule, to know The Rule, or even
to think that The Rule is a good idea.  The Founder of SQLite believes
that anyone who follows The Rule will live a happier and more productive 
life, but individuals are free to dispute or ignore that advice if
they wish.
The founder of SQLite and all
current developers have pledged to follow the spirit of The Rule
to the best of their ability. They
view The Rule as their promise to all SQLite users of how the developers
are expected to behave.
This is a one-way promise, or covenant.
In other words, the developers are saying: "We will treat you this
way regardless of how you treat us."
3. The Rule
 First of all, love the Lord God with your whole heart,
     your whole soul, and your whole strength.
 Then, love your neighbor as yourself.
 Do not murder.
 Do not commit adultery.
 Do not steal.
 Do not covet.
 Do not bear false witness.
 Honor all people.
 Do not do to another what you would not have done to yourself.
 Deny oneself in order to follow Christ.
 Chastise the body.
 Do not become attached to pleasures.
 Love fasting.
 Relieve the poor.
 Clothe the naked.
 Visit the sick.
 Bury the dead.
 Be a help in times of trouble.
 Console the sorrowing.
 Be a stranger to the world's ways.
 Prefer nothing more than the love of Christ.
 Do not give way to anger.
 Do not nurse a grudge.
 Do not entertain deceit in your heart.
 Do not give a false peace.
 Do not forsake charity.
 Do not swear, for fear of perjuring yourself.
 Utter only truth from heart and mouth.
 Do not return evil for evil.
 Do no wrong to anyone, and bear patiently wrongs done to yourself.
 Love your enemies.
 Do not curse those who curse you, but rather bless them.
 Bear persecution for justice's sake.
 Be not proud.
 Be not addicted to wine.
 Be not a great eater.
 Be not drowsy.
 Be not lazy.
 Be not a grumbler.
 Be not a detractor.
 Put your hope in God.
 Attribute to God, and not to self, whatever good you see in yourself.
 Recognize always that evil is your own doing,
     and to impute it to yourself.
 Fear the Day of Judgment.
 Be in dread of hell.
 Desire eternal life with all the passion of the spirit.
 Keep death daily before your eyes.
 Keep constant guard over the actions of your life.
 Know for certain that God sees you everywhere.
 When wrongful thoughts come into your heart, dash them against
     Christ immediately.
 Disclose wrongful thoughts to your spiritual mentor.
 Guard your tongue against evil and depraved speech.
 Do not love much talking.
 Speak no useless words or words that move to laughter.
 Do not love much or boisterous laughter.
 Listen willingly to holy reading.
 Devote yourself frequently to prayer.
 Daily in your prayers, with tears and sighs, confess your
     past sins to God, and amend them for the future.
 Fulfill not the desires of the flesh; hate your own will.
 Obey in all things the commands of those whom God has placed
     in authority over you even though they (which God forbid) should 
     act otherwise, mindful of the Lord's precept, "Do what they say, 
     but not what they do."
 Do not wish to be called holy before one is holy; but first to be
     holy, that you may be truly so called.
 Fulfill God's commandments daily in your deeds.
 Love chastity.
 Hate no one.
 Be not jealous, nor harbor envy.
 Do not love quarreling.
 Shun arrogance.
 Respect your seniors.
 Love your juniors.
 Pray for your enemies in the love of Christ.
 Make peace with your adversary before the sun sets.
 Never despair of God's mercy.
